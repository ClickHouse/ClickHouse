import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

import pytest
import logging
import os
import json
import time

import pyspark
import delta
from delta import *
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
    BooleanType,
    ArrayType,
)
from pyspark.sql.functions import current_timestamp
from datetime import datetime

from helpers.s3_tools import prepare_s3_bucket, upload_directory, get_file_contents


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TABLE_NAME = "test_delta_table"
USER_FILES_PATH = "/ClickHouse/tests/integration/test_storage_delta/_instances/node1/database/user_files"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node1", with_minio=True)

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)

        yield cluster

    finally:
        cluster.shutdown()


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local")
    )

    return configure_spark_with_delta_pip(builder).master("local").getOrCreate()


def get_delta_metadata(delta_metadata_file):
    jsons = [json.loads(x) for x in delta_metadata_file.splitlines()]
    combined_json = {}
    for d in jsons:
        combined_json.update(d)
    return combined_json


def test_basic(started_cluster):
    instance = started_cluster.instances["node1"]

    data_path = f"/var/lib/clickhouse/user_files/{TABLE_NAME}.parquet"
    inserted_data = "SELECT number, toString(number) FROM numbers(100)"
    instance.query(
        f"INSERT INTO TABLE FUNCTION file('{data_path}') {inserted_data} FORMAT Parquet"
    )

    instance.exec_in_container(
        ["bash", "-c", "chmod 777 -R /var/lib/clickhouse/user_files"],
        user="root",
    )

    spark = get_spark()
    result_path = f"/{TABLE_NAME}_result"

    spark.read.load(f"file://{USER_FILES_PATH}/{TABLE_NAME}.parquet").write.mode(
        "overwrite"
    ).option("compression", "none").format("delta").option(
        "delta.columnMapping.mode", "name"
    ).save(
        result_path
    )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    upload_directory(minio_client, bucket, result_path, "")

    data = get_file_contents(
        minio_client,
        bucket,
        "/test_delta_table_result/_delta_log/00000000000000000000.json",
    )
    delta_metadata = get_delta_metadata(data)

    stats = json.loads(delta_metadata["add"]["stats"])
    assert stats["numRecords"] == 100
    assert next(iter(stats["minValues"].values())) == 0
    assert next(iter(stats["maxValues"].values())) == 99

    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/test_delta_table_result/', 'minio', 'minio123')"""
    )
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


def test_types(started_cluster):
    spark = get_spark()
    result_file = f"{TABLE_NAME}_result_2"
    delta_table = (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT")
        .addColumn("b", "STRING")
        .addColumn("c", "DATE")
        .addColumn("d", "ARRAY<STRING>")
        .addColumn("e", "BOOLEAN")
        .execute()
    )
    data = [
        (
            123,
            "string",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["str1", "str2"],
            True,
        )
    ]

    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType()),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.write.mode("append").format("delta").saveAsTable(TABLE_NAME)

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    upload_directory(minio_client, bucket, f"/{result_file}", "")

    instance = started_cluster.instances["node1"]
    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"""
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', 'minio123')"
    assert (
        instance.query(f"SELECT * FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date32)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
        ]
    )
