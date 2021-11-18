from builtins import range
from datetime import date, timedelta

import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from requests.exceptions import HTTPError
from requests.structures import CaseInsensitiveDict

###################################### CONFIG ##################################

# date TODO: manage it better
adate = date.today()
hadoop_partition = adate.strftime("%Y/%m/%d")

# API
URL = 
AUTH_ENDPOINT =
CREDENTIALS = 
DATA_ENDPOINT = 

# Postgres
# $ psql -U pguser -d dshop < /tmp/dshop_dump.sql
# $ psql -U pguser -d dshop // passwd
# \dt+
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_USER = "pguser"
POSTGRES_PASSWORD = "secret"
POSTGRES_DB = "dshop"
POSTGRES_TABLES = ["aisles", "clients", "departments", "orders", "products"]
SCHEMA = "public"
POSTGRES_CONN_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_DRIVER = "/home/user/airflow/dags/etl_pipeline/etl/postgresql-42.2.24.jar"

# Hadoop
HDFS_DATALAKE = f"hdfs://localhost:9000/data/{hadoop_partition}/"
COLLECT_META = {
    "api": {"format": "json", "fname": HDFS_DATALAKE + "out_of_stock.json"},
    "db": {"format": "parquet", "fname": HDFS_DATALAKE + "%s.parquet"},
}

# Greenplum
GP_DRIVER = "/home/user/airflow/dags/etl_pipeline/etl/greenplum-connector-apache-spark-scala_2.12-2.1.0.jar"
GP_HOST = "localhost"
GP_PORT = 5433
GP_DB = "dshop"
GP_USER = "gpuser"
GP_PASSWORD = "secret"


###################################### COMMON ##################################

# TODO: spark context manager?
def create_spark_session():
    return (
        SparkSession.builder.appName("ETL_Pipeline")
        .config("spark.jars", ",".join((POSTGRES_DRIVER, GP_DRIVER)))
        .getOrCreate()
    )


def store_df_with_overwrite(df, fmt, destination):
    df.write.mode("overwrite").format(fmt).save(destination)


############################ COLLECT FROM API ##################################


def _get_token() -> str:
    """Returns auth token."""
    url = URL + AUTH_ENDPOINT
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    try:
        response = requests.post(
            url,
            data=CREDENTIALS,
            headers=headers,
        )
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    else:
        return response.json().get("access_token")


def _get_out_of_stock_products(token: str, date: str) -> dict:
    """Returns list of out-of-stock products given a date in a format `YYYY-MM-DD`"""
    url = URL + DATA_ENDPOINT
    params = {"date": date}

    headers = CaseInsensitiveDict()
    headers["Authorization"] = f"JWT {token}"

    try:
        response = requests.get(
            url,
            headers=headers,
            params=params,
        )
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    else:
        # TODO: cleanup JSON if we use per-date data, reduce to id only?
        # sample: {'product_id': 32042, 'date': '2021-11-11'}
        return response.json()


def collect_from_api() -> None:
    date = adate.strftime("%Y-%m-%d")
    token = _get_token()
    data = _get_out_of_stock_products(token, date)

    spark = create_spark_session()
    df = spark.createDataFrame(data=data, schema=["product_id", "date"])
    store_df_with_overwrite(
        df=df,
        fmt=COLLECT_META["api"]["format"],
        destination=COLLECT_META["api"]["fname"],
    )
    spark.stop()


############################# COLLECT FROM DB ##################################


def _process_db_data(spark, table: str):
    """Loads data from a Postgres table into dataframe."""
    pythonReadOptions = {
        "url": POSTGRES_CONN_URL,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "dbtable": table,
        "driver": "org.postgresql.Driver"
    }
    return spark.read.format("jdbc").options(**pythonReadOptions).load()


def collect_from_db() -> None:
    spark = create_spark_session()
    # TODO: parallelize?
    fmt = COLLECT_META["db"]["format"]
    destination = COLLECT_META["db"]["fname"]

    for table in POSTGRES_TABLES:
        table_with_schema = ".".join((SCHEMA, table))
        df = _process_db_data(spark, table_with_schema)
        store_df_with_overwrite(df, fmt, destination % table_with_schema)

    spark.stop()


########################## Move to Data Warehouse ##############################


def _get_sources() -> list:
    """
    Returns the list of sources to move to Data Warehouse.
    [
        {"source": <file_name>, "destination": <table_name.date>, "format": <fmt>},
        ...
    ]
    """
    day = adate.strftime("%Y%m%d")
    # API part
    sources = [
        {
            "source": COLLECT_META["api"]["fname"],
            "destination": f"out_of_stock_{day}",
            "format": COLLECT_META["api"]["format"],
        }
    ]
    # DB part
    source_template = COLLECT_META["db"]["fname"]
    fmt = COLLECT_META["db"]["format"]
    for table in POSTGRES_TABLES:
        sources.append(
            {
                "source": source_template % ".".join((SCHEMA, table)),
                "destination": f"{table}_{day}",
                "format": fmt,
            }
        )

    return sources


def move_to_dwh() -> None:
    gscPythonWriteOptions = {
        "url": f"jdbc:postgresql://{GP_HOST}:{GP_PORT}/{GP_DB}",
        "user": GP_USER,
        "password": GP_PASSWORD,
        "dbschema": SCHEMA,
        "driver": "org.postgresql.Driver"
    }

    spark = create_spark_session()
    for source in _get_sources():
        # source= _get_sources()[0]
        src, destination, source_fmt = (
            source["source"],
            source["destination"],
            source["format"],
        )
        df = spark.read.format(source_fmt).load(src)
        df.write.format("jdbc").options(
            **gscPythonWriteOptions, dbtable=destination
        ).mode("Overwrite").save()

    spark.stop()


################################### DAG START ##################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="DataPipeline",
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval="@daily",
    max_active_runs=1,
    orientation="TB",
    catchup=False,
) as dag:

    collect_from_api_task = PythonOperator(
        task_id="collect_from_api",
        python_callable=collect_from_api,
        dag=dag,
    )

    collect_from_db_task = PythonOperator(
        task_id="collect_from_db",
        python_callable=collect_from_db,
        dag=dag,
    )

    move_to_dwh_task = PythonOperator(
        task_id="move_to_dwh",
        python_callable=move_to_dwh,  # TODO
        dag=dag,
    )

collect_from_db >> collect_from_api >>  move_to_dwh_task

if __name__ == "__main__":
    dag.cli()
