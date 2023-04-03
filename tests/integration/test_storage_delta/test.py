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
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

from helpers.s3_tools import prepare_s3_bucket, upload_directory, get_file_contents


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
USER_FILES_PATH = os.path.join(SCRIPT_DIR, "./_instances/node1/database/user_files")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            with_minio=True,
        )

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


def write_delta_from_file(spark, path, result_path, mode="overwrite"):
    spark.read.load(path).write.mode(mode).option("compression", "none").format(
        "delta"
    ).option("delta.columnMapping.mode", "name").save(result_path)


def write_delta_from_df(spark, df, result_path, mode="overwrite", partition_by=None):
    if partition_by is None:
        df.write.mode(mode).option("compression", "none").format("delta").option(
            "delta.columnMapping.mode", "name"
        ).save(result_path)
    else:
        df.write.mode(mode).option("compression", "none").format("delta").option(
            "delta.columnMapping.mode", "name"
        ).partitionBy("a").save(result_path)


def generate_data(spark, start, end):
    a = spark.range(start, end, 1).toDF("a")
    b = spark.range(start + 1, end + 1, 1).toDF("b")
    b = b.withColumn("b", b["b"].cast(StringType()))

    a = a.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    b = b.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    df = a.join(b, on=["row_index"]).drop("row_index")
    return df


def get_delta_metadata(delta_metadata_file):
    jsons = [json.loads(x) for x in delta_metadata_file.splitlines()]
    combined_json = {}
    for d in jsons:
        combined_json.update(d)
    return combined_json


def create_delta_table(node, table_name):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=DeltaLake(s3, filename = '{table_name}/')"""
    )


def create_initial_data_file(node, query, table_name, compression_method="none"):
    node.query(
        f"""
        INSERT INTO TABLE FUNCTION
            file('{table_name}.parquet')
        SETTINGS
            output_format_parquet_compression_method='{compression_method}',
            s3_truncate_on_insert=1 {query}
        FORMAT Parquet"""
    )
    result_path = f"{USER_FILES_PATH}/{table_name}.parquet"
    return result_path


def test_single_log_file(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    spark = get_spark()
    TABLE_NAME = "test_single_log_file"

    inserted_data = "SELECT number, toString(number + 1) FROM numbers(100)"
    parquet_data_path = create_initial_data_file(instance, inserted_data, TABLE_NAME)

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2  # 1 metadata files + 1 data file

    create_delta_table(instance, TABLE_NAME)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


def test_partition_by(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    spark = get_spark()
    TABLE_NAME = "test_partition_by"

    write_delta_from_df(
        spark,
        generate_data(spark, 0, 10),
        f"/{TABLE_NAME}",
        mode="overwrite",
        partition_by="a",
    )

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 11 # 10 partitions and 1 metadata file

    create_delta_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10


def test_multiple_log_files(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    spark = get_spark()
    TABLE_NAME = "test_multiple_log_files"

    write_delta_from_df(
        spark, generate_data(spark, 0, 100), f"/{TABLE_NAME}", mode="overwrite"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2  # 1 metadata files + 1 data file

    s3_objects = list(
        minio_client.list_objects(bucket, f"/{TABLE_NAME}/_delta_log/", recursive=True)
    )
    assert len(s3_objects) == 1

    create_delta_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_delta_from_df(
        spark, generate_data(spark, 100, 200), f"/{TABLE_NAME}", mode="append"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 4  # 2 metadata files + 2 data files

    s3_objects = list(
        minio_client.list_objects(bucket, f"/{TABLE_NAME}/_delta_log/", recursive=True)
    )
    assert len(s3_objects) == 2

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )


def test_metadata(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    spark = get_spark()
    TABLE_NAME = "test_metadata"

    parquet_data_path = create_initial_data_file(
        instance, "SELECT number, toString(number) FROM numbers(100)", TABLE_NAME
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    data = get_file_contents(
        minio_client,
        bucket,
        f"/{TABLE_NAME}/_delta_log/00000000000000000000.json",
    )
    delta_metadata = get_delta_metadata(data)

    stats = json.loads(delta_metadata["add"]["stats"])
    assert stats["numRecords"] == 100
    assert next(iter(stats["minValues"].values())) == 0
    assert next(iter(stats["maxValues"].values())) == 99

    create_delta_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


def test_types(started_cluster):
    spark = get_spark()
    TABLE_NAME = "test_types"
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
