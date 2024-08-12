from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'create_crypto_schema_and_table',
    default_args=default_args,
    description='A DAG to create schema and table for crypto data',
    schedule_interval=None,  # Set to your desired schedule
    start_date=days_ago(1),
    catchup=False,
) as dag:

    create_schema = PostgresOperator(
        task_id='create_schema',
        postgres_conn_id='your_postgres_connection_id',
        sql="""
        CREATE SCHEMA IF NOT EXISTS crypto;
        """,
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='your_postgres_connection_id',
        sql="""
        CREATE TABLE IF NOT EXISTS crypto.assets (
            id VARCHAR(50) NOT NULL,
            rank INTEGER,
            symbol VARCHAR(50),
            name VARCHAR(50),
            supply NUMERIC,
            max_supply NUMERIC,
            market_cap NUMERIC,
            volume_24hr NUMERIC,
            price NUMERIC,
            change_per_24hr NUMERIC,
            volume_we_24hr NUMERIC,
            update_utc TIMESTAMP WITH TIME ZONE
        );
        """,
    )

    create_schema >> create_table
