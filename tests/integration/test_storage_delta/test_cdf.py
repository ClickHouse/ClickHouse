import json
import logging
import os
import random
import string
import time
import threading
from datetime import datetime

import delta
import pyspark
import pytest
from delta import *
from pyspark.sql.functions import (
    col,
    current_timestamp,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    DecimalType,
    StructField,
    StructType,
    TimestampType,
)
from decimal import Decimal
from pyspark.sql.window import Window

import helpers.client
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.network import PartitionManager
from helpers.s3_tools import (
    LocalUploader,
    S3Uploader,
    get_file_contents,
    list_s3_objects,
    prepare_s3_bucket,
    upload_directory,
    LocalDownloader,
)
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config


SCRIPT_DIR = "/var/lib/clickhouse/user_files" + os.path.join(
    os.path.dirname(os.path.realpath(__file__))
)
cluster = ClickHouseCluster(__file__, with_spark=True, azurite_default_port=10000)

S3_DATA = [
    "field_ids_struct_test/data/00000-1-7cad83a6-af90-42a9-8a10-114cbc862a42-0-00001.parquet",
]


def get_spark(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            "/var/lib/clickhouse/user_files",
        )
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.master("local").getOrCreate()


def randomize_table_name(table_name, random_suffix_length=10):
    letters = string.ascii_letters + string.digits
    return f"{table_name}{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "instance1",
            main_configs=[
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/disable_s3_retries.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
                "configs/users.d/enable_writes.xml",
            ],
            with_minio=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        if int(cluster.instances["instance1"].query("SELECT count() FROM system.table_engines WHERE name = 'DeltaLake'").strip()) == 0:
            pytest.skip(
                "DeltaLake engine is not available"
            )

        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.minio_restricted_bucket = "{}-with-auth".format(cluster.minio_bucket)
        if cluster.minio_client.bucket_exists(cluster.minio_restricted_bucket):
            cluster.minio_client.remove_bucket(cluster.minio_restricted_bucket)

        cluster.minio_client.make_bucket(cluster.minio_restricted_bucket)

        # Only support local delta tables on the first node for now
        # extend this if testing on other nodes becomes necessary
        cluster.local_uploader = LocalUploader(cluster.instances["instance1"])

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(cluster.instances_dir)
        )

        for file in S3_DATA:
            print(f"Copying object {file}")
            cluster.minio_client.fput_object(
                bucket_name=cluster.minio_bucket,
                object_name=file,
                file_path=os.path.join(
                    os.path.join(os.path.dirname(os.path.realpath(__file__))), file
                ),
            )

        yield cluster

    finally:
        cluster.shutdown()




@pytest.mark.parametrize("column_mapping", [""])
def test_cdf(started_cluster, column_mapping):
    instance = started_cluster.instances["instance1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    table_name = randomize_table_name("test_cdf")
    partition_columns = ["year"]
    minio_client = started_cluster.minio_client
    spark = started_cluster.spark_session
    num_rows = 10
    path = f"/{table_name}"

    # Omit _commit_timestamp
    select_columns = "first_name, age, _change_type, _commit_version"

    # Commit version 0
    df = spark.createDataFrame([("aa", 11), ("ab", 22), ("ac", 33)]).toDF(
        "first_name", "age"
    )

    df.write.format("delta").partitionBy("age").save(path)

    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")

    # Commit version 1
    spark.sql(
        f"""
ALTER TABLE {table_name}
SET TBLPROPERTIES ('delta.minReaderVersion'='1', 'delta.minWriterVersion'='2', delta.enableChangeDataFeed = true)
            """
    )
    df = spark.createDataFrame([("ba", 44), ("bb", 55), ("bc", 66)]).toDF(
        "first_name", "age"
    )
    # Commit version 2
    df.write.format("delta").mode("append").partitionBy("age").save(path)
    upload_directory(minio_client, bucket, path, "")

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{table_name}/', 'minio', '{minio_secret_key}')"
    assert (
        "aa\t11\n"
        "ab\t22\n"
        "ac\t33\n"
        "ba\t44\n"
        "bb\t55\n"
        "bc\t66"
        == instance.query(
            f"SELECT * FROM {table_function} ORDER BY all",
        ).strip()
    )
    assert (
        "first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)"
        == instance.query(f"describe table {table_function}").strip()
    )

    # We turned on enableChangeDataFeed only since commit version 2.
    assert (
        "Delta lake CDF is allowed only for deltaLake table function"
        in instance.query_and_get_error(
            f"CREATE TABLE a ENGINE = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{table_name}/', 'minio', '{minio_secret_key}')",
            settings={"delta_lake_snapshot_start_version": 0},
        )
    )
    # Data with CDF enabled starts from snapshot version 2.
    # Snapshot version 1 is just a metadata change. 
    # Reading from snapshot version 1 can sometimes fail with "cdf not enabled", because metadata is propagated asynchronously.
    assert (
        "ba\t44\tinsert\t2\n"
        "bb\t55\tinsert\t2\n"
        "bc\t66\tinsert\t2"
        == instance.query(
            f"SELECT {select_columns} FROM {table_function} ORDER BY all",
            settings={"delta_lake_snapshot_start_version": 2},
        ).strip()
    )

    assert (
        "first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)\t\t\t\t\t\n_change_type\tString\t\t\t\t\t\n_commit_version\tInt64\t\t\t\t\t\n_commit_timestamp\tDateTime64(6)"
        == instance.query(
            f"describe table {table_function}",
            settings={"delta_lake_snapshot_start_version": 2},
        ).strip()
    )

    df = spark.createDataFrame([("ca", 77), ("cb", 88), ("cc", 99)]).toDF(
        "first_name", "age"
    )

    df.write.format("delta").mode("append").partitionBy("age").save(path)
    upload_directory(minio_client, bucket, path, "")

    assert (
        "ca\t77\tinsert\t3\n"
        "cb\t88\tinsert\t3\n"
        "cc\t99\tinsert\t3"
        == instance.query(
            f"SELECT {select_columns} FROM {table_function} ORDER BY all",
            settings={"delta_lake_snapshot_start_version": 3},
        ).strip()
    )

    df = spark.createDataFrame(
        [("da", 1, "qq"), ("db", 2, "qq"), ("dc", 3, "qq")]
    ).toDF("first_name", "age", "country")

    df.write.option("mergeSchema", "true").format("delta").mode("append").partitionBy(
        "age"
    ).save(path)
    upload_directory(minio_client, bucket, path, "")

    select_columns_altered = "first_name, age, country, _change_type, _commit_version"
    assert (
        "da\t1\tqq\tinsert\t4\n"
        "db\t2\tqq\tinsert\t4\n"
        "dc\t3\tqq\tinsert\t4"
        == instance.query(
            f"SELECT {select_columns_altered} FROM {table_function} ORDER BY all",
            settings={"delta_lake_snapshot_start_version": 4},
        ).strip()
    )
    assert (
        "first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)\t\t\t\t\t\ncountry\tNullable(String)\t\t\t\t\t\n_change_type\tString\t\t\t\t\t\n_commit_version\tInt64\t\t\t\t\t\n_commit_timestamp\tDateTime64(6)"
        == instance.query(
            f"describe table {table_function}",
            settings={"delta_lake_snapshot_start_version": 4},
        ).strip()
    )

    assert (
        "ba\t44\tinsert\t2\n"
        "bb\t55\tinsert\t2\n"
        "bc\t66\tinsert\t2\n"
        "ca\t77\tinsert\t3\n"
        "cb\t88\tinsert\t3\n"
        "cc\t99\tinsert\t3"
        == instance.query(
            f"SELECT {select_columns} FROM {table_function} ORDER BY all",
            settings={
                "delta_lake_snapshot_start_version": 1,
                "delta_lake_snapshot_end_version": 3,
            },
        ).strip()
    )
    assert (
        "first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)\t\t\t\t\t\n_change_type\tString\t\t\t\t\t\n_commit_version\tInt64\t\t\t\t\t\n_commit_timestamp\tDateTime64(6)"
        == instance.query(
            f"describe table {table_function}",
            settings={
                "delta_lake_snapshot_start_version": 1,
                "delta_lake_snapshot_end_version": 3,
            },
        ).strip()
    )

    assert (
        "ba\t44\tinsert\t2\n"
        "bc\t66\tinsert\t2\n"
        "ca\t77\tinsert\t3\n"
        "cc\t99\tinsert\t3"
        == instance.query(
            f"SELECT {select_columns} FROM {table_function} WHERE age != 55 and age != 88 ORDER BY all",
            settings={
                "delta_lake_snapshot_start_version": 1,
                "delta_lake_snapshot_end_version": 3,
            },
        ).strip()
    )
    assert (
        "Generic delta kernel error: Failed to build TableChanges: Start and end version schemas are different."
        in instance.query_and_get_error(
            f"SELECT {select_columns} FROM {table_function} ORDER BY all",
            settings={
                "delta_lake_snapshot_start_version": 1,
                "delta_lake_snapshot_end_version": 4,
            },
        ).strip()
    )

    assert (
        "da\t1\tqq\tinsert\t4\n"
        "db\t2\tqq\tinsert\t4\n"
        "dc\t3\tqq\tinsert\t4"
        == instance.query(
            f"SELECT {select_columns_altered} FROM {table_function} ORDER BY all",
            settings={
                "delta_lake_snapshot_start_version": 4,
                "delta_lake_snapshot_end_version": 4,
            },
        ).strip()
    )
