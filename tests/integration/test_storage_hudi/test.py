import json
import logging
import os
from datetime import datetime

import pyspark
import pytest
from pyspark.sql.functions import (
    current_timestamp,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

import helpers.client
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_tools import get_file_contents, prepare_s3_bucket, upload_directory
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.catalog.local", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
        )
        .config("spark.driver.memory", "20g")
        .master("local")
    )
    return builder.getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__, with_spark=True)
    try:
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = get_spark()

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def write_hudi_from_df(spark, table_name, df, result_path, mode="overwrite"):
    if mode == "overwrite":
        hudi_write_mode = "insert_overwrite"
    else:
        hudi_write_mode = "upsert"

    df.write.mode(mode).option("compression", "none").option(
        "compression", "none"
    ).format("hudi").option("hoodie.table.name", table_name).option(
        "hoodie.datasource.write.partitionpath.field", "partitionpath"
    ).option(
        "hoodie.datasource.write.table.name", table_name
    ).option(
        "hoodie.datasource.write.operation", hudi_write_mode
    ).option(
        "hoodie.datasource.compaction.async.enable", "true"
    ).option(
        "hoodie.compact.inline", "false"
    ).option(
        "hoodie.compact.inline.max.delta.commits", "10"
    ).option(
        "hoodie.parquet.compression.codec", "snappy"
    ).option(
        "hoodie.hfile.compression.algorithm", "uncompressed"
    ).option(
        "hoodie.datasource.write.recordkey.field", "a"
    ).option(
        "hoodie.datasource.write.precombine.field", "a"
    ).save(
        result_path
    )


def write_hudi_from_file(spark, table_name, path, result_path):
    spark.conf.set("spark.sql.debug.maxToStringFields", 100000)
    df = spark.read.load(f"file://{path}")
    write_hudi_from_df(spark, table_name, df, result_path)


def generate_data(spark, start, end, append=1):
    a = spark.range(start, end, 1).toDF("a")
    b = spark.range(start + append, end + append, 1).toDF("b")
    b = b.withColumn("b", b["b"].cast(StringType()))

    a = a.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    b = b.withColumn(
        "row_index", row_number().over(Window.orderBy(monotonically_increasing_id()))
    )

    df = a.join(b, on=["row_index"]).drop("row_index")
    return df


def create_hudi_table(node, table_name):
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name}
        ENGINE=Hudi(s3, filename = '{table_name}/')"""
    )


def create_initial_data_file(
    cluster, node, query, table_name, compression_method="none"
):
    node.query(
        f"""
        INSERT INTO TABLE FUNCTION
            file('{table_name}.parquet')
        SETTINGS
            output_format_parquet_compression_method='{compression_method}',
            s3_truncate_on_insert=1 {query}
        FORMAT Parquet"""
    )
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


def test_single_hudi_file(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_single_hudi_file"

    inserted_data = "SELECT number as a, toString(number) as b FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME
    )

    write_hudi_from_file(spark, TABLE_NAME, parquet_data_path, f"/{TABLE_NAME}")
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 1
    assert files[0].endswith(".parquet")

    create_hudi_table(instance, TABLE_NAME)
    assert instance.query(f"SELECT a, b FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


def test_multiple_hudi_files(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_multiple_hudi_files"

    write_hudi_from_df(
        spark, TABLE_NAME, generate_data(spark, 0, 100), f"/{TABLE_NAME}"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 1

    create_hudi_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 200),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(
        f"SELECT a, b FROM {TABLE_NAME} ORDER BY 1"
    ) == instance.query("SELECT number, toString(number + 1) FROM numbers(200)")

    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 300),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 3

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 300
    assert instance.query(
        f"SELECT a, b FROM {TABLE_NAME} ORDER BY 1"
    ) == instance.query("SELECT number, toString(number + 1) FROM numbers(300)")

    assert int(instance.query(f"SELECT b FROM {TABLE_NAME} WHERE a = 100")) == 101
    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 101, append=0),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 300
    assert int(instance.query(f"SELECT b FROM {TABLE_NAME} WHERE a = 100")) == 100

    write_hudi_from_df(
        spark,
        TABLE_NAME,
        generate_data(spark, 100, 1000000, append=0),
        f"/{TABLE_NAME}",
        mode="append",
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1000000


def test_types(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = "test_types"

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
    write_hudi_from_df(spark, TABLE_NAME, df, f"/{TABLE_NAME}", mode="overwrite")

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    create_hudi_table(instance, TABLE_NAME)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function = f"hudi(s3, filename='{TABLE_NAME}/')"
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["_hoodie_commit_time", "Nullable(String)"],
            ["_hoodie_commit_seqno", "Nullable(String)"],
            ["_hoodie_record_key", "Nullable(String)"],
            ["_hoodie_partition_path", "Nullable(String)"],
            ["_hoodie_file_name", "Nullable(String)"],
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date32)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
        ]
    )
