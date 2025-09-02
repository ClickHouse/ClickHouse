import glob
import json
import logging
import os
import random
import string
import time
import uuid
import threading
from datetime import datetime
from multiprocessing.dummy import Pool

import delta
import pyarrow as pa
import pyarrow.parquet as pq
import pyspark
import pytest
from azure.storage.blob import BlobServiceClient
from delta import *
from deltalake.writer import write_deltalake
from minio.deleteobjects import DeleteObject
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
    AzureUploader,
    LocalUploader,
    S3Uploader,
    get_file_contents,
    list_s3_objects,
    prepare_s3_bucket,
    upload_directory,
    LocalDownloader,
    LocalUploader,
)
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, with_spark=True)

S3_DATA = [
    "field_ids_struct_test/data/00000-1-7cad83a6-af90-42a9-8a10-114cbc862a42-0-00001.parquet",
]


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .master("local")
    )

    return builder.master("local").getOrCreate()


def randomize_table_name(table_name, random_suffix_length=10):
    letters = string.ascii_letters + string.digits
    return f"{table_name}{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
                "configs/users.d/enable_writes.xml",
            ],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
            with_zookeeper=True,
            macros={"shard": 0, "replica": 1},
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
                "configs/users.d/enable_writes.xml",
            ],
            with_minio=True,
            stay_alive=True,
            with_zookeeper=True,
            with_remote_database_disk=False,  # Disable `with_remote_database_disk` as in `test_replicated_database_and_unavailable_s3``, minIO rejects node2 connections
            macros={"shard": 0, "replica": 2},
        )
        cluster.add_instance(
            "node_with_environment_credentials",
            with_minio=True,
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/use_environment_credentials.xml",
            ],
            env_variables={
                "AWS_ACCESS_KEY_ID": minio_access_key,
                "AWS_SECRET_ACCESS_KEY": minio_secret_key,
            },
            with_remote_database_disk=False,
        )
        cluster.add_instance(
            "node_old",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag="25.3.3.42",
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_with_disabled_delta_kernel",
            main_configs=[
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
                "configs/config.d/remote_servers.xml",
            ],
            user_configs=[
                "configs/users.d/users.xml",
                "configs/users.d/disabled_delta_kernel.xml",
            ],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.minio_restricted_bucket = "{}-with-auth".format(cluster.minio_bucket)
        if cluster.minio_client.bucket_exists(cluster.minio_restricted_bucket):
            cluster.minio_client.remove_bucket(cluster.minio_restricted_bucket)

        cluster.minio_client.make_bucket(cluster.minio_restricted_bucket)

        cluster.azure_container_name = "mycontainer"
        cluster.blob_service_client = cluster.blob_service_client
        container_client = cluster.blob_service_client.create_container(
            cluster.azure_container_name
        )
        cluster.container_client = container_client
        cluster.default_azure_uploader = AzureUploader(
            cluster.blob_service_client, cluster.azure_container_name
        )

        # Only support local delta tables on the first node for now
        # extend this if testing on other nodes becomes necessary
        cluster.local_uploader = LocalUploader(cluster.instances["node1"])

        cluster.spark_session = get_spark()

        for file in S3_DATA:
            print(f"Copying object {file}")
            cluster.minio_client.fput_object(
                bucket_name=cluster.minio_bucket,
                object_name=file,
                file_path=os.path.join(SCRIPT_DIR, file),
            )

        yield cluster

    finally:
        cluster.shutdown()


def write_delta_from_file(spark, path, result_path, mode="overwrite"):
    spark.read.load(path).write.mode(mode).option("compression", "none").format(
        "delta"
    ).option("delta.columnMapping.mode", "name").save(result_path)


def write_delta_from_df(
    spark, df, result_path, mode="overwrite", partition_by=None, column_mapping="name"
):
    if partition_by is None:
        df.write.mode(mode).option("compression", "none").option(
            "delta.columnMapping.mode", column_mapping
        ).format("delta").save(result_path)
    else:
        df.write.mode(mode).option("compression", "none").format("delta").option(
            "delta.columnMapping.mode", column_mapping
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


def get_storage_options(cluster):
    return {
        "AWS_ENDPOINT_URL": f"http://{cluster.minio_ip}:{cluster.minio_port}",
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def get_delta_metadata(delta_metadata_file):
    jsons = [json.loads(x) for x in delta_metadata_file.splitlines()]
    combined_json = {}
    for d in jsons:
        combined_json.update(d)
    return combined_json


def get_node(cluster, use_delta_kernel):
    if use_delta_kernel == "1":
        return cluster.instances["node1"]
    elif use_delta_kernel == "0":
        return cluster.instances["node_with_disabled_delta_kernel"]
    else:
        assert False


def create_delta_table(
    instance,
    storage_type,
    table_name,
    cluster,
    format="Parquet",
    **kwargs,
):

    if storage_type == "s3":
        if "bucket" in kwargs:
            bucket = kwargs["bucket"]
        else:
            bucket = cluster.minio_bucket

        instance.query(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
            ENGINE=DeltaLake(s3, filename = '{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')
            """
        )

    elif storage_type == "azure":
        instance.query(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
            ENGINE=DeltaLakeAzure(azure, container = {cluster.azure_container_name}, storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/{table_name}', format={format})
            """
        )
    elif storage_type == "local":
        # For local storage, we need to use the absolute path
        user_files_path = os.path.join(
            SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
        )
        table_path = os.path.join(user_files_path, table_name)
        instance.query(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
            ENGINE=DeltaLakeLocal('{table_path}', {format})
            """
        )
    else:
        raise Exception(f"Unknown delta lake storage type: {storage_type}")


def default_upload_directory(
    started_cluster, storage_type, local_path, remote_path, **kwargs
):
    if storage_type == "s3":
        print(kwargs)
        return started_cluster.default_s3_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    elif storage_type == "azure":
        return started_cluster.default_azure_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    elif storage_type == "local":
        return started_cluster.local_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    else:
        raise Exception(f"Unknown delta storage type: {storage_type}")


def create_initial_data_file(
    cluster, node, query, table_name, compression_method="none", node_name="node1"
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
        SCRIPT_DIR, f"{cluster.instances_dir_name}/{node_name}/database/user_files"
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


@pytest.mark.parametrize(
    "use_delta_kernel, storage_type",
    [("1", "s3"), ("0", "s3"), ("0", "azure"), ("1", "local")],
)
def test_single_log_file(started_cluster, use_delta_kernel, storage_type):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    TABLE_NAME = randomize_table_name("test_single_log_file")

    inserted_data = "SELECT number as a, toString(number + 1) as b FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster, instance, inserted_data, TABLE_NAME, node_name=instance.name
    )

    # For local storage, we need to use the absolute path
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/{instance.name}/database/user_files"
    )
    table_path = os.path.join(user_files_path, TABLE_NAME)

    # We need to exclude the leading slash for local storage protocol file://
    delta_path = table_path if storage_type == "local" else f"/{TABLE_NAME}"
    write_delta_from_file(spark, parquet_data_path, delta_path)

    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        "",
    )

    assert len(files) == 2  # 1 metadata files + 1 data file

    create_delta_table(
        instance,
        storage_type,
        TABLE_NAME,
        started_cluster,
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )


@pytest.mark.parametrize(
    "use_delta_kernel, storage_type",
    [("1", "s3"), ("0", "s3"), ("0", "azure"), ("1", "local")],
)
def test_partition_by(started_cluster, use_delta_kernel, storage_type):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session

    TABLE_NAME = randomize_table_name("test_partition_by")

    # For local storage, we need to use the absolute path
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/{instance.name}/database/user_files"
    )
    table_path = os.path.join(user_files_path, TABLE_NAME)

    # We need to exclude the leading slash for local storage protocol file://
    delta_path = table_path if storage_type == "local" else f"/{TABLE_NAME}"

    write_delta_from_df(
        spark,
        generate_data(spark, 0, 10),
        delta_path,
        mode="overwrite",
        partition_by="a",
    )

    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        "",
    )

    assert len(files) == 11  # 10 partitions and 1 metadata file

    create_delta_table(
        instance,
        storage_type,
        TABLE_NAME,
        started_cluster,
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10


@pytest.mark.parametrize(
    "use_delta_kernel, storage_type",
    [("1", "s3"), ("0", "s3"), ("0", "azure"), ("1", "local")],
)
def test_checkpoint(started_cluster, use_delta_kernel, storage_type):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_checkpoint")

    # For local storage, we need to use the absolute path
    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/{instance.name}/database/user_files"
    )
    table_path = os.path.join(user_files_path, TABLE_NAME)
    # We need to exclude the leading slash for local storage protocol file://
    delta_path = table_path if storage_type == "local" else f"/{TABLE_NAME}"

    write_delta_from_df(
        spark,
        generate_data(spark, 0, 1),
        delta_path,
        mode="overwrite",
    )
    for i in range(1, 25):
        write_delta_from_df(
            spark,
            generate_data(spark, i, i + 1),
            delta_path,
            mode="append",
        )

    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        "",
    )
    # 25 data files
    # 25 metadata files
    # 1 last_metadata file
    # 2 checkpoints
    assert len(files) == 25 * 2 + 3

    ok = False
    for file in files:
        if file.endswith("last_checkpoint"):
            ok = True
    assert ok

    create_delta_table(
        instance,
        storage_type,
        TABLE_NAME,
        started_cluster,
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} SETTINGS input_format_parquet_allow_missing_columns=1"
            )
        )
        == 25
    )

    table = DeltaTable.forPath(spark, delta_path)
    table.delete("a < 10")
    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        "",
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 15

    for i in range(0, 5):
        write_delta_from_df(
            spark,
            generate_data(spark, i, i + 1),
            delta_path,
            mode="append",
        )
    # + 1 metadata files (for delete)
    # + 5 data files
    # + 5 metadata files
    # + 1 checkpoint file
    # + 1 ?
    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        "",
    )
    assert len(files) == 53 + 1 + 5 * 2 + 1 + 1
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 20

    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1").strip()
        == instance.query(
            "SELECT * FROM ("
            "SELECT number, toString(number + 1) FROM numbers(5) "
            "UNION ALL SELECT number, toString(number + 1) FROM numbers(10, 15) "
            ") ORDER BY 1"
        ).strip()
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_multiple_log_files(started_cluster, use_delta_kernel):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_multiple_log_files")

    write_delta_from_df(
        spark, generate_data(spark, 0, 100), f"/{TABLE_NAME}", mode="overwrite"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 2  # 1 metadata files + 1 data file

    s3_objects = list(
        minio_client.list_objects(bucket, f"{TABLE_NAME}/_delta_log/", recursive=True)
    )
    assert len(s3_objects) == 1

    create_delta_table(instance, "s3", TABLE_NAME, started_cluster)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_delta_from_df(
        spark, generate_data(spark, 100, 200), f"/{TABLE_NAME}", mode="append"
    )
    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) == 4  # 2 metadata files + 2 data files

    s3_objects = list(
        minio_client.list_objects(bucket, f"{TABLE_NAME}/_delta_log/", recursive=True)
    )
    assert len(s3_objects) == 2

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_metadata(started_cluster, use_delta_kernel):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_metadata")

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=instance.name,
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

    create_delta_table(instance, "s3", TABLE_NAME, started_cluster)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_types(started_cluster, use_delta_kernel):
    instance = get_node(started_cluster, use_delta_kernel)
    TABLE_NAME = randomize_table_name("test_types")
    spark = started_cluster.spark_session
    result_file = randomize_table_name(f"{TABLE_NAME}_result_2")

    delta_table = (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT", nullable=True)
        .addColumn("b", "STRING", nullable=False)
        .addColumn("c", "DATE", nullable=False)
        .addColumn("d", "ARRAY<STRING>", nullable=False)
        .addColumn("e", "BOOLEAN", nullable=True)
        .addColumn("f", ArrayType(StringType(), containsNull=False), nullable=False)
        .execute()
    )
    data = [
        (
            123,
            "string",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["str1", "str2"],
            True,
            ["str1", "str4"],
        )
    ]

    schema = StructType(
        [
            StructField("a", IntegerType(), nullable=True),
            StructField("b", StringType(), nullable=False),
            StructField("c", DateType(), nullable=False),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType(), nullable=False),
            StructField("f", ArrayType(StringType(), containsNull=False)),
        ]
    )
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.write.mode("append").format("delta").saveAsTable(TABLE_NAME)

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    upload_directory(minio_client, bucket, f"/{result_file}", "")

    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"""
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT * FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue\t['str1','str4']"
    )

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    assert (
        instance.query(f"SELECT * FROM {table_function}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue\t['str1','str4']"
    )

    assert instance.query(f"DESCRIBE {table_function} FORMAT TSV") == TSV(
        [
            ["a", "Nullable(Int32)"],
            ["b", "String"],
            ["c", "Date32"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
            ["f", "Array(String)"],
        ]
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_restart_broken(started_cluster, use_delta_kernel):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = "broken"
    TABLE_NAME = randomize_table_name("test_restart_broken")

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=instance.name,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    create_delta_table(
        instance,
        "s3",
        TABLE_NAME,
        started_cluster,
        bucket=bucket,
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    s3_objects = list_s3_objects(minio_client, bucket, prefix="")
    assert (
        len(
            list(
                minio_client.remove_objects(
                    bucket,
                    [DeleteObject(obj) for obj in s3_objects],
                )
            )
        )
        == 0
    )
    minio_client.remove_bucket(bucket)

    instance.restart_clickhouse()

    assert "NoSuchBucket" in instance.query_and_get_error(
        f"SELECT count() FROM {TABLE_NAME}"
    )

    s3_disk_no_key_errors_metric_value = int(
        instance.query(
            """
            SELECT value
            FROM system.metrics
            WHERE metric = 'DiskS3NoSuchKeyErrors'
            """
        ).strip()
    )

    assert s3_disk_no_key_errors_metric_value == 0

    minio_client.make_bucket(bucket)

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_restart_broken_table_function(started_cluster, use_delta_kernel):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = "broken2"
    TABLE_NAME = randomize_table_name("test_restart_broken_table_function")

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=instance.name,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    instance.query(
        f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME}
        AS deltaLake(s3, filename = '{TABLE_NAME}/', url = 'http://minio1:9001/{bucket}/')"""
    )
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    s3_objects = list_s3_objects(minio_client, bucket, prefix="")
    assert (
        len(
            list(
                minio_client.remove_objects(
                    bucket,
                    [DeleteObject(obj) for obj in s3_objects],
                )
            )
        )
        == 0
    )
    minio_client.remove_bucket(bucket)

    instance.restart_clickhouse()

    assert "NoSuchBucket" in instance.query_and_get_error(
        f"SELECT count() FROM {TABLE_NAME}"
    )

    minio_client.make_bucket(bucket)

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


@pytest.mark.parametrize(
    "use_delta_kernel, cluster",
    [("1", False), ("1", True)],
    # [("1", False), ("1", True), ("0", False)], /// FIXME: this does not work with disabled delta-kernel
)
def test_partition_columns(started_cluster, use_delta_kernel, cluster):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_partition_columns")
    result_file = f"{TABLE_NAME}"
    partition_columns = ["b", "c", "d", "e", "f", "g"]

    delta_table = (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT")
        .addColumn("b", "STRING")
        .addColumn("c", "DATE")
        .addColumn("d", "INT")
        .addColumn("e", "TIMESTAMP")
        .addColumn("f", "BOOLEAN")
        .addColumn("g", "DECIMAL(10,2)")
        .addColumn("h", "BOOLEAN")
        .partitionedBy(partition_columns)
        .execute()
    )
    num_rows = 9

    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", IntegerType()),
            StructField("e", TimestampType()),
            StructField("f", BooleanType()),
            StructField("g", DecimalType(10, 2)),
            StructField("h", BooleanType()),
        ]
    )

    now = datetime.now()
    for i in range(1, num_rows + 1):
        data = [
            (
                i,
                "test" + str(i),
                datetime.strptime(f"2000-01-0{i}", "%Y-%m-%d"),
                i,
                (
                    now
                    if i % 2 == 0
                    else datetime.strptime(
                        f"2012-01-0{i} 12:34:56.789123", "%Y-%m-%d %H:%M:%S.%f"
                    )
                ),
                True if i % 2 == 0 else False,
                Decimal(f"{i * 1.11:.2f}"),
                False if i % 2 == 0 else True,
            )
        ]
        df = spark.createDataFrame(data=data, schema=schema)
        df.printSchema()
        df.write.mode("append").format("delta").partitionBy(partition_columns).save(
            f"/{TABLE_NAME}"
        )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")

    if cluster:
        table_function = f"deltaLakeCluster(cluster, 'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    else:
        table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    result = instance.query(f"describe table {table_function}").strip()
    assert (
        result == "a\tNullable(Int32)\t\t\t\t\t\n"
        "b\tNullable(String)\t\t\t\t\t\n"
        "c\tNullable(Date32)\t\t\t\t\t\n"
        "d\tNullable(Int32)\t\t\t\t\t\n"
        "e\tNullable(DateTime64(6))\t\t\t\t\t\n"
        "f\tNullable(Bool)\t\t\t\t\t\n"
        "g\tNullable(Decimal(10, 2))\t\t\t\t\t\n"
        "h\tNullable(Bool)"
    )

    result = int(instance.query(f"SELECT count() FROM {table_function}"))
    assert result == num_rows

    expected_output = f"""1	test1	2000-01-01	1	2012-01-01 12:34:56.789123	false	1.11	true
2	test2	2000-01-02	2	{now}	true	2.22	false
3	test3	2000-01-03	3	2012-01-03 12:34:56.789123	false	3.33	true
4	test4	2000-01-04	4	{now}	true	4.44	false
5	test5	2000-01-05	5	2012-01-05 12:34:56.789123	false	5.55	true
6	test6	2000-01-06	6	{now}	true	6.66	false
7	test7	2000-01-07	7	2012-01-07 12:34:56.789123	false	7.77	true
8	test8	2000-01-08	8	{now}	true	8.88	false
9	test9	2000-01-09	9	2012-01-09 12:34:56.789123	false	9.99	true"""

    assert (
        expected_output
        == instance.query(f"SELECT * FROM {table_function} ORDER BY b").strip()
    )

    query_id = f"query_with_filter_{TABLE_NAME}"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE c == toDateTime('2000/01/05')
            """,
            query_id=query_id,
        )
    )
    assert result == 1

    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE e = toDateTime64('{now}', 6)
            """,
            query_id=query_id,
        )
    )
    assert result == 4

    if use_delta_kernel == 1:
        instance.query("SYSTEM FLUSH LOGS")
        assert num_rows - 1 == int(
            instance.query(
                f"""
            SELECT ProfileEvents['DeltaLakePartitionPrunedFiles']
            FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        """
            )
        )

    instance.query(
        f"""
       DROP TABLE IF EXISTS {TABLE_NAME};
       CREATE TABLE {TABLE_NAME} (a Nullable(Int32), b Nullable(String), c Nullable(Date32), d Nullable(Int32), h Nullable(Bool))
       ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')
        """
    )
    assert (
        """1	test1	2000-01-01	1	true
2	test2	2000-01-02	2	false
3	test3	2000-01-03	3	true
4	test4	2000-01-04	4	false
5	test5	2000-01-05	5	true
6	test6	2000-01-06	6	false
7	test7	2000-01-07	7	true
8	test8	2000-01-08	8	false
9	test9	2000-01-09	9	true"""
        == instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY b").strip()
    )

    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} WHERE c == toDateTime('2000/01/05')"
            )
        )
        == 1
    )

    # Subset of columns should work.
    instance.query(
        f"""
       DROP TABLE IF EXISTS {TABLE_NAME};
       CREATE TABLE {TABLE_NAME} (b Nullable(String), c Nullable(Date32), d Nullable(Int32))
       ENGINE=DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')
       """
    )
    assert (
        """test1	2000-01-01	1
test2	2000-01-02	2
test3	2000-01-03	3
test4	2000-01-04	4
test5	2000-01-05	5
test6	2000-01-06	6
test7	2000-01-07	7
test8	2000-01-08	8
test9	2000-01-09	9"""
        == instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY b").strip()
    )

    for i in range(num_rows + 1, 2 * num_rows + 1):
        data = [
            (
                i,
                "test" + str(i),
                datetime.strptime(f"2000-01-{i}", "%Y-%m-%d"),
                i,
                (
                    now
                    if i % 2 == 0
                    else datetime.strptime(
                        f"2012-01-{i} 12:34:56.789123", "%Y-%m-%d %H:%M:%S.%f"
                    )
                ),
                True if i % 2 == 0 else False,
                Decimal(f"{i * 1.1:.2f}"),
                False if i % 2 == 0 else True,
            )
        ]
        df = spark.createDataFrame(data=data, schema=schema)
        df.printSchema()
        df.write.mode("append").format("delta").partitionBy(partition_columns).save(
            f"/{TABLE_NAME}"
        )

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    ok = False
    for file in files:
        if file.endswith("last_checkpoint"):
            ok = True
    assert ok

    result = int(instance.query(f"SELECT count() FROM {table_function}"))
    assert result == num_rows * 2

    assert (
        f"""1	test1	2000-01-01	1	2012-01-01 12:34:56.789123	false	1.11	true
2	test2	2000-01-02	2	{now}	true	2.22	false
3	test3	2000-01-03	3	2012-01-03 12:34:56.789123	false	3.33	true
4	test4	2000-01-04	4	{now}	true	4.44	false
5	test5	2000-01-05	5	2012-01-05 12:34:56.789123	false	5.55	true
6	test6	2000-01-06	6	{now}	true	6.66	false
7	test7	2000-01-07	7	2012-01-07 12:34:56.789123	false	7.77	true
8	test8	2000-01-08	8	{now}	true	8.88	false
9	test9	2000-01-09	9	2012-01-09 12:34:56.789123	false	9.99	true
10	test10	2000-01-10	10	{now}	true	11	false
11	test11	2000-01-11	11	2012-01-11 12:34:56.789123	false	12.1	true
12	test12	2000-01-12	12	{now}	true	13.2	false
13	test13	2000-01-13	13	2012-01-13 12:34:56.789123	false	14.3	true
14	test14	2000-01-14	14	{now}	true	15.4	false
15	test15	2000-01-15	15	2012-01-15 12:34:56.789123	false	16.5	true
16	test16	2000-01-16	16	{now}	true	17.6	false
17	test17	2000-01-17	17	2012-01-17 12:34:56.789123	false	18.7	true
18	test18	2000-01-18	18	{now}	true	19.8	false"""
        == instance.query(f"SELECT * FROM {table_function} ORDER BY c").strip()
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {TABLE_NAME} WHERE c == toDateTime('2000/01/15')"
            )
        )
        == 1
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_complex_types(started_cluster, use_delta_kernel):
    node = get_node(started_cluster, use_delta_kernel)
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            (
                "address",
                pa.struct(
                    [
                        ("street", pa.string()),
                        ("city", pa.string()),
                        ("state", pa.string()),
                    ]
                ),
            ),
            ("interests", pa.list_(pa.string())),
            (
                "metadata",
                pa.map_(
                    pa.string(), pa.string()
                ),  # Map with string keys and string values
            ),
        ]
    )

    # Create sample data
    data = [
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["John Doe", "Jane Smith", "Jake Johnson"], type=pa.string()),
        pa.array(
            [
                {"street": "123 Elm St", "city": "Springfield", "state": "IL"},
                {"street": "456 Maple St", "city": "Shelbyville", "state": "IL"},
                {"street": "789 Oak St", "city": "Ogdenville", "state": "IL"},
            ],
            type=schema.field("address").type,
        ),
        pa.array(
            [
                pa.array(["dancing", "coding", "hiking"]),
                pa.array(["dancing", "coding", "hiking"]),
                pa.array(["dancing", "coding", "hiking"]),
            ],
            type=schema.field("interests").type,
        ),
        pa.array(
            [
                {"key1": "value1", "key2": "value2"},
                {"key1": "value3", "key2": "value4"},
                {"key1": "value5", "key2": "value6"},
            ],
            type=schema.field("metadata").type,
        ),
    ]

    endpoint_url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"
    aws_access_key_id = "minio"
    aws_secret_access_key = "ClickHouse_Minio_P@ssw0rd"
    table_name = randomize_table_name("test_complex_types")

    storage_options = {
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ACCESS_KEY_ID": aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    path = f"s3://root/{table_name}"
    table = pa.Table.from_arrays(data, schema=schema)

    write_deltalake(path, table, storage_options=storage_options)

    assert "1\n2\n3\n" in node.query(
        f"SELECT id FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' , 'minio', '{minio_secret_key}')"
    )
    assert (
        "('123 Elm St','Springfield','IL')\n('456 Maple St','Shelbyville','IL')\n('789 Oak St','Ogdenville','IL')"
        in node.query(
            f"SELECT address FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' , 'minio', '{minio_secret_key}')"
        )
    )
    assert (
        "{'key1':'value1','key2':'value2'}\n{'key1':'value3','key2':'value4'}\n{'key1':'value5','key2':'value6'}"
        in node.query(
            f"SELECT metadata FROM deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' , 'minio', '{minio_secret_key}')"
        )
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_filesystem_cache(started_cluster, use_delta_kernel):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    TABLE_NAME = randomize_table_name("test_filesystem_cache")
    bucket = started_cluster.minio_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT toUInt64(number), toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=instance.name,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    create_delta_table(instance, "s3", TABLE_NAME, started_cluster)

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {TABLE_NAME} SETTINGS filesystem_cache_name = 'cache1'",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    count = int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 < int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {TABLE_NAME} SETTINGS filesystem_cache_name = 'cache1'",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    assert count == int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_replicated_database_and_unavailable_s3(started_cluster, use_delta_kernel):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]

    DB_NAME = randomize_table_name("db")
    TABLE_NAME = randomize_table_name("test_replicated_database_and_unavailable_s3")
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_restricted_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    node1.query(
        f"CREATE DATABASE {DB_NAME} ENGINE=Replicated('/clickhouse/databases/{DB_NAME}', 'shard1', 'node1')"
    )
    node2.query(
        f"CREATE DATABASE {DB_NAME} ENGINE=Replicated('/clickhouse/databases/{DB_NAME}', 'shard1', 'node2')"
    )

    parquet_data_path = create_initial_data_file(
        started_cluster,
        node1,
        "SELECT number, toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=node1.name,
    )

    endpoint_url = f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}"
    aws_access_key_id = "minio"
    aws_secret_access_key = "ClickHouse_Minio_P@ssw0rd"

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("name", pa.string()),
        ]
    )

    data = [
        pa.array([1, 2, 3], type=pa.int32()),
        pa.array(["John Doe", "Jane Smith", "Jake Johnson"], type=pa.string()),
    ]
    storage_options = {
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ACCESS_KEY_ID": aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    path = f"s3://root/{TABLE_NAME}"
    table = pa.Table.from_arrays(data, schema=schema)

    write_deltalake(path, table, storage_options=storage_options)

    with PartitionManager() as pm:
        pm_rule_reject = {
            "probability": 1,
            "destination": node2.ip_address,
            "source_port": started_cluster.minio_port,
            "action": "REJECT --reject-with tcp-reset",
        }
        pm_rule_drop_all = {
            "destination": node2.ip_address,
            "source_port": started_cluster.minio_port,
            "action": "DROP",
        }
        pm._add_rule(pm_rule_reject)

        node1.query(
            f"""
            DROP TABLE IF EXISTS {DB_NAME}.{TABLE_NAME};
            CREATE TABLE {DB_NAME}.{TABLE_NAME}
            AS deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{TABLE_NAME}' , 'minio', '{minio_secret_key}')
            """
        )

        assert TABLE_NAME in node1.query(
            f"select name from system.tables where database = '{DB_NAME}'"
        )
        assert TABLE_NAME in node2.query(
            f"select name from system.tables where database = '{DB_NAME}'"
        )

        replica_path = f"/clickhouse/databases/{DB_NAME}/replicas/shard1|node2"
        zk = started_cluster.get_kazoo_client("zoo1")
        zk.set(replica_path + "/digest", "123456".encode())

        assert "123456" in node2.query(
            f"SELECT * FROM system.zookeeper WHERE path = '{replica_path}'"
        )

        node2.restart_clickhouse()

        assert "123456" not in node2.query(
            f"SELECT * FROM system.zookeeper WHERE path = '{replica_path}'"
        )


def test_session_token(started_cluster):
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    TABLE_NAME = randomize_table_name("test_session_token")
    bucket = started_cluster.minio_bucket

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    node_name = "node_with_environment_credentials"
    instance = started_cluster.instances[node_name]
    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        "SELECT toUInt64(number), toString(number) FROM numbers(100)",
        TABLE_NAME,
        node_name=node_name,
    )

    write_delta_from_file(spark, parquet_data_path, f"/{TABLE_NAME}")
    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")

    assert 0 < int(
        instance.query(
            f"""
    SELECT count() FROM deltaLake(
        'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{TABLE_NAME}/',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """
        )
    )

    instance2 = started_cluster.instances["node1"]

    assert 0 < int(
        instance2.query(
            f"""
    SELECT count() FROM deltaLake(
        'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{TABLE_NAME}/', '{minio_access_key}', '{minio_secret_key}',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """
        )
    )

    assert (
        "Received DeltaLake kernel error ObjectStoreError: Error interacting with object store"
        in instance2.query_and_get_error(
            f"""
    SELECT count() FROM deltaLake(
        'http://{started_cluster.minio_host}:{started_cluster.minio_port}/{started_cluster.minio_bucket}/{TABLE_NAME}/', '{minio_access_key}', '{minio_secret_key}', 'fake-token',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """
        )
    )


@pytest.mark.parametrize("cluster", [False, True])
def test_partition_columns_2(started_cluster, cluster):
    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_partition_columns_2")

    schema = pa.schema(
        [
            ("a", pa.int32()),
            ("b", pa.int32()),
            ("c", pa.int32()),
            ("d", pa.string()),
            ("e", pa.string()),
        ]
    )
    data = [
        pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        pa.array([4, 5, 6, 7, 8], type=pa.int32()),
        pa.array([7, 7, 8, 9, 10], type=pa.int32()),
        pa.array(["aa", "bb", "cc", "aa", "bb"], type=pa.string()),
        pa.array(["aa", "bb", "cc", "aa", "cc"], type=pa.string()),
    ]

    storage_options = {
        "AWS_ENDPOINT_URL": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    path = f"s3://root/{table_name}"
    table = pa.Table.from_arrays(data, schema=schema)

    write_deltalake(
        path, table, storage_options=storage_options, partition_by=["c", "d"]
    )

    if cluster:
        delta_function = f"""
    deltaLakeCluster(
             cluster,
            'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
            '{minio_access_key}',
            '{minio_secret_key}')
        """
    else:
        delta_function = f"""
    deltaLake(
            'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
            '{minio_access_key}',
            '{minio_secret_key}',
        SETTINGS allow_experimental_delta_kernel_rs=0)
        """

    num_files = int(
        node.query(
            f"SELECT uniqExact(_path) FROM {delta_function}",
            settings={"allow_experimental_delta_kernel_rs": 1},
        )
    )
    assert num_files == 5

    new_data = [
        pa.array([2], type=pa.int32()),
        pa.array([3], type=pa.int32()),
        pa.array([7], type=pa.int32()),
        pa.array(["aa"], type=pa.string()),
        pa.array(["cc"], type=pa.string()),
    ]
    new_table_data = pa.Table.from_arrays(new_data, schema=schema)

    write_deltalake(
        path, new_table_data, storage_options=storage_options, mode="append"
    )

    assert (
        "a\tNullable(Int32)\t\t\t\t\t\n"
        "b\tNullable(Int32)\t\t\t\t\t\n"
        "c\tNullable(Int32)\t\t\t\t\t\n"
        "d\tNullable(String)\t\t\t\t\t\n"
        "e\tNullable(String)"
        == node.query(
            f"DESCRIBE TABLE {delta_function}",
            settings={"allow_experimental_delta_kernel_rs": 1},
        ).strip()
    )

    num_files = int(
        node.query(
            f"SELECT uniqExact(_path) FROM {delta_function}",
            settings={"allow_experimental_delta_kernel_rs": 1},
        )
    )
    assert num_files == 6

    query_id = f"{table_name}-{uuid.uuid4()}"

    assert (
        "1\t4\t7\taa\taa\n2\t3\t7\taa\tcc\n2\t5\t7\tbb\tbb\n3\t6\t8\tcc\tcc\n4\t7\t9\taa\taa\n5\t8\t10\tbb\tcc"
        == node.query(
            f" SELECT * FROM {delta_function} ORDER BY all",
            settings={
                "allow_experimental_delta_kernel_rs": 1,
                "use_hive_partitioning": 0,
            },
        ).strip()
    )

    assert (
        "1"
        in node.query(
            f" SELECT a FROM {delta_function} WHERE c = 7 and d = 'aa'",
            query_id=query_id,
            settings={
                "allow_experimental_delta_kernel_rs": 1,
                "delta_lake_enable_engine_predicate": 0,
            },
        ).strip()
    )

    def check_pruned(count, query_id):
        node.query("SYSTEM FLUSH LOGS")
        assert count == int(
            node.query(
                f"""
            SELECT ProfileEvents['DeltaLakePartitionPrunedFiles']
            FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        """
            )
        )

    check_pruned(num_files - 2, query_id)

    query_id = f"{table_name}-{uuid.uuid4()}"
    assert (
        "2"
        in node.query(
            f"SELECT a FROM {delta_function} WHERE c = 7 and d = 'bb'",
            query_id=query_id,
            settings={
                "allow_experimental_delta_kernel_rs": 1,
                "delta_lake_enable_engine_predicate": 0,
            },
        ).strip()
    )

    check_pruned(num_files - 1, query_id)


@pytest.mark.parametrize(
    "column_mapping", ["", "name"]
)  # "id" is not supported by delta-kernel at the moment
def test_rename_and_add_column(started_cluster, column_mapping):
    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_rename_column")
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    path = f"/{table_name}"

    df = spark.createDataFrame([("alice", 47), ("anora", 23), ("aelin", 51)]).toDF(
        "first_name", "age"
    )

    if column_mapping == "":
        df.write.format("delta").partitionBy("age").save(path)
    else:
        df.write.format("delta").partitionBy("age").option(
            "delta.minReaderVersion", "2"
        ).option("delta.minWriterVersion", "5").option(
            "delta.columnMapping.mode", column_mapping
        ).save(
            path
        )

    upload_directory(minio_client, bucket, path, "")

    delta_function = f"""
deltaLake(
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """

    def check_schema(expected):
        assert expected == node.query(f"DESCRIBE TABLE {delta_function}").strip()

    def check_data(expected):
        assert (
            expected
            == node.query(f"SELECT * FROM {delta_function} ORDER BY all").strip()
        )

    def append_data(df):
        df.write.option("mergeSchema", "true").mode("append").format(
            "delta"
        ).partitionBy("age").save(path)
        upload_directory(minio_client, bucket, path, "")

    def check_pruned_files(expected, query_id):
        node.query("SYSTEM FLUSH LOGS")
        assert expected == int(
            node.query(
                f"""
            SELECT ProfileEvents['DeltaLakePartitionPrunedFiles']
            FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        """
            )
        )

    check_schema("first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)")
    check_data("aelin\t51\n" "alice\t47\n" "anora\t23")

    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")

    if column_mapping == "":
        # To allow column rename
        spark.sql(
            f"""
ALTER TABLE {table_name}
SET TBLPROPERTIES ('delta.minReaderVersion'='2', 'delta.minWriterVersion'='5', 'delta.columnMapping.mode' = 'name')
                """
        )

    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN first_name TO naam")

    df = spark.createDataFrame([("bob", 12), ("bill", 33), ("bober", 49)]).toDF(
        "naam", "age"
    )
    append_data(df)

    assert "Unknown expression identifier `first_name`" in node.query_and_get_error(
        f"SELECT first_name FROM {delta_function} WHERE age = 51"
    )

    check_schema("naam\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)")
    check_data(
        "aelin\t51\n" "alice\t47\n" "anora\t23\n" "bill\t33\n" "bob\t12\n" "bober\t49"
    )

    query_id = f"{table_name}-{uuid.uuid4()}"
    assert (
        "bob"
        == node.query(
            f"SELECT naam FROM {delta_function} WHERE age = 12",
            query_id=query_id,
            settings={
                "allow_experimental_delta_kernel_rs": 1,
                "delta_lake_enable_engine_predicate": 0,
            },
        ).strip()
    )
    check_pruned_files(5, query_id)

    query_id = f"{table_name}-{uuid.uuid4()}"
    assert (
        "aelin"
        == node.query(
            f"SELECT naam FROM {delta_function} WHERE age = 51",
            query_id=query_id,
            settings={
                "allow_experimental_delta_kernel_rs": 1,
                "delta_lake_enable_engine_predicate": 0,
            },
        ).strip()
    )
    check_pruned_files(5, query_id)

    df = spark.createDataFrame([("cicil", 68, "usa"), ("corsha", 26, "chaol")]).toDF(
        "naam", "age", "country"
    )

    df.write.option("mergeSchema", "true").mode("append").format("delta").partitionBy(
        "age"
    ).save(path)

    upload_directory(minio_client, bucket, path, "")

    assert (
        "naam\tNullable(String)\t\t\t\t\t\n"
        "age\tNullable(Int64)\t\t\t\t\t\n"
        "country\tNullable(String)"
        == node.query(f"DESCRIBE TABLE {delta_function}").strip()
    )

    assert (
        "aelin\t51\t\\N\n"
        "alice\t47\t\\N\n"
        "anora\t23\t\\N\n"
        "bill\t33\t\\N\n"
        "bob\t12\t\\N\n"
        "bober\t49\t\\N\n"
        "cicil\t68\tusa\n"
        "corsha\t26\tchaol"
        == node.query(f"SELECT * FROM {delta_function} ORDER BY all").strip()
    )

    df = spark.createDataFrame([("engineer", 32)]).toDF("profession", "age")

    df.write.option("mergeSchema", "true").mode("append").format("delta").partitionBy(
        "age"
    ).save(path)

    upload_directory(minio_client, bucket, path, "")

    assert (
        "aelin\t51\t\\N\t\\N\n"
        "alice\t47\t\\N\t\\N\n"
        "anora\t23\t\\N\t\\N\n"
        "bill\t33\t\\N\t\\N\n"
        "bob\t12\t\\N\t\\N\n"
        "bober\t49\t\\N\t\\N\n"
        "cicil\t68\tusa\t\\N\n"
        "corsha\t26\tchaol\t\\N\n"
        "\\N\t32\t\\N\tengineer"
        == node.query(f"SELECT * FROM {delta_function} ORDER BY all").strip()
    )

    paths = (
        node.query(f"SELECT _path FROM {delta_function} ORDER BY all")
        .strip()
        .splitlines()
    )

    def s3_function(path):
        return f""" s3(
            'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{path}' ,
            '{minio_access_key}',
            '{minio_secret_key}')
        """

    assert len(paths) == 9

    schemas = dict()
    for path in paths:
        schema = node.query(f"DESCRIBE TABLE {s3_function(path)}").strip()
        if schema in schemas:
            schemas[schema].append(path)
        else:
            schemas[schema] = [path]

    assert len(schemas) == 3
    counts = []
    for schema, schema_paths in schemas.items():
        counts.append(len(schema_paths))
    counts.sort()

    assert counts == [1, 2, 6]


def test_alter_column_type(started_cluster):
    ## Delta lake supports a very limited set of type changes:
    ## https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column#parameters-1
    ## What is done in this test:
    ## Alter Short -> Int
    ## Alter Int -> Nullable(Int)
    ##
    ## Complex type changes are supported only with data overwrite
    ## https://docs.delta.io/latest/delta-batch.html#change-column-type-or-name

    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_rename_column")
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    path = f"/{table_name}"

    delta_function = f"""
deltaLake(
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """

    def check_schema(expected):
        assert node.query(f"DESCRIBE TABLE {delta_function} FORMAT TSV") == TSV(
            expected
        )

    def check_data(expected):
        assert (
            expected
            == node.query(f"SELECT * FROM {delta_function} ORDER BY all").strip()
        )

    def append_data(df):
        df.write.option("mergeSchema", "true").mode("append").format(
            "delta"
        ).partitionBy("age").save(path)
        upload_directory(minio_client, bucket, path, "")

    delta_table = (
        DeltaTable.create(spark)
        .tableName(table_name)
        .location(path)
        .addColumn("a", "SHORT", nullable=False)
        .addColumn("b", "STRING", nullable=False)
        .addColumn("c", "DATE", nullable=False)
        .addColumn("d", "ARRAY<STRING>", nullable=False)
        .addColumn("e", "BOOLEAN", nullable=True)
        .addColumn("f", ArrayType(StringType(), containsNull=False), nullable=False)
        .partitionedBy("c")
        .property("delta.minReaderVersion", "2")
        .property("delta.minWriterVersion", "5")
        .property("delta.columnMapping.mode", "name")
        .execute()
    )

    data = [
        (
            1,
            "a",
            datetime.strptime("2000-01-01", "%Y-%m-%d"),
            ["aa", "aa"],
            True,
            ["aaa", "aaa"],
        )
    ]

    schema = StructType(
        [
            StructField("a", ShortType(), nullable=True),
            StructField("b", StringType(), nullable=False),
            StructField("c", DateType(), nullable=False),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType(), nullable=False),
            StructField("f", ArrayType(StringType(), containsNull=False)),
        ]
    )

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.format("delta").partitionBy("c").mode("overwrite").save(path)

    upload_directory(minio_client, bucket, path, "")

    check_schema(
        [
            ["a", "Int16"],
            ["b", "String"],
            ["c", "Date32"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
            ["f", "Array(String)"],
        ]
    )

    schema = StructType(
        [
            StructField("a", IntegerType(), nullable=False),
            StructField("b", StringType(), nullable=False),
            StructField("c", DateType(), nullable=False),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType(), nullable=False),
            StructField("f", ArrayType(StringType(), containsNull=False)),
        ]
    )

    data = [
        (
            214748364,
            "b",
            datetime.strptime("2000-02-02", "%Y-%m-%d"),
            ["bb", "bb"],
            False,
            ["bbb", "bbb"],
        )
    ]

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.option("mergeSchema", "true").mode("append").format("delta").partitionBy(
        "c"
    ).save(path)

    upload_directory(minio_client, bucket, path, "")

    check_schema(
        [
            ["a", "Int32"],
            ["b", "String"],
            ["c", "Date32"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
            ["f", "Array(String)"],
        ]
    )

    assert (
        "1\ta\t2000-01-01\t['aa','aa']\ttrue\t['aaa','aaa']\n214748364\tb\t2000-02-02\t['bb','bb']\tfalse\t['bbb','bbb']\n"
        == node.query(f"SELECT * FROM {delta_function} ORDER BY all")
    )

    spark.sql(f"ALTER TABLE {table_name} CHANGE COLUMN a DROP NOT NULL;")
    schema = StructType(
        [
            StructField("a", IntegerType(), nullable=True),
            StructField("b", StringType(), nullable=False),
            StructField("c", DateType(), nullable=False),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType(), nullable=False),
            StructField("f", ArrayType(StringType(), containsNull=False)),
        ]
    )

    data = [
        (
            None,
            "c",
            datetime.strptime("2000-03-03", "%Y-%m-%d"),
            ["cc", "cc"],
            False,
            ["ccc", "ccc"],
        )
    ]

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.option("mergeSchema", "true").mode("append").format("delta").partitionBy(
        "c"
    ).save(path)

    upload_directory(minio_client, bucket, path, "")
    check_schema(
        [
            ["a", "Nullable(Int32)"],
            ["b", "String"],
            ["c", "Date32"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
            ["f", "Array(String)"],
        ]
    )
    assert (
        "1\ta\t2000-01-01\t['aa','aa']\ttrue\t['aaa','aaa']\n214748364\tb\t2000-02-02\t['bb','bb']\tfalse\t['bbb','bbb']\n\\N\tc\t2000-03-03\t['cc','cc']\tfalse\t['ccc','ccc']\n"
        == node.query(f"SELECT * FROM {delta_function} ORDER BY all")
    )

    paths = (
        node.query(f"SELECT _path FROM {delta_function} ORDER BY all")
        .strip()
        .splitlines()
    )

    def s3_function(path):
        return f""" s3(
            'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{path}' ,
            '{minio_access_key}',
            '{minio_secret_key}')
        """

    assert len(paths) == 3

    assert "Nullable(Int16)" in node.query(
        f"DESCRIBE TABLE {s3_function(paths[0])}"
    ) or "Nullable(Int16)" in node.query(f"DESCRIBE TABLE {s3_function(paths[1])}")

    assert "Nullable(Int32)" in node.query(
        f"DESCRIBE TABLE {s3_function(paths[0])}"
    ) or "Nullable(Int32)" in node.query(f"DESCRIBE TABLE {s3_function(paths[1])}")

    schema = StructType(
        [
            StructField("a", StringType(), nullable=True),
            StructField("b", StringType(), nullable=False),
            StructField("c", DateType(), nullable=False),
            StructField("d", ArrayType(StringType())),
            StructField("e", BooleanType(), nullable=False),
            StructField("f", ArrayType(StringType(), containsNull=False)),
        ]
    )

    data = [
        (
            "123",
            "d",
            datetime.strptime("2000-04-04", "%Y-%m-%d"),
            ["ddd", "dd"],
            False,
            ["ddd", "ddd"],
        )
    ]

    spark.read.table(table_name).withColumn("a", col("a").cast("String")).write.format(
        "delta"
    ).mode("overwrite").option("overwriteSchema", "true").partitionBy("c").save(path)

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.mode("append").format("delta").partitionBy("c").save(path)

    upload_directory(minio_client, bucket, path, "")

    # spark.read.table(table_name).printSchema()
    check_schema(
        [
            ["a", "Nullable(String)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date32)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
            ["f", "Array(Nullable(String))"],
        ]
    )
    assert (
        "1\ta\t2000-01-01\t['aa','aa']\ttrue\t['aaa','aaa']\n123\td\t2000-04-04\t['ddd','dd']\tfalse\t['ddd','ddd']\n214748364\tb\t2000-02-02\t['bb','bb']\tfalse\t['bbb','bbb']\n\\N\tc\t2000-03-03\t['cc','cc']\tfalse\t['ccc','ccc']\n"
        == node.query(
            f"SELECT * FROM {delta_function} ORDER BY all settings input_format_parquet_allow_missing_columns=0 "
        )
    )


@pytest.mark.parametrize("new_analyzer", ["1", "0"])
def test_cluster_function(started_cluster, new_analyzer):
    instance = started_cluster.instances["node1"]
    instance_old = started_cluster.instances["node_old"]
    table_name = randomize_table_name("test_cluster_function")

    schema = pa.schema([("a", pa.int32()), ("b", pa.string())])
    data = [
        pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        pa.array(["aa", "bb", "cc", "aa", "bb"], type=pa.string()),
    ]

    storage_options = {
        "AWS_ENDPOINT_URL": f"http://{started_cluster.minio_ip}:{started_cluster.minio_port}",
        "AWS_ACCESS_KEY_ID": minio_access_key,
        "AWS_SECRET_ACCESS_KEY": minio_secret_key,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    path = f"s3://root/{table_name}"
    table = pa.Table.from_arrays(data, schema=schema)
    write_deltalake(path, table, storage_options=storage_options, partition_by=["b"])

    table_function = f"""
deltaLakeCluster(cluster,
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """
    instance.query(
        f"SELECT * FROM {table_function} SETTINGS allow_experimental_analyzer={new_analyzer}"
    )
    assert 5 == int(
        instance.query(
            f"SELECT count() FROM {table_function} SETTINGS allow_experimental_analyzer={new_analyzer}"
        )
    )
    assert "1\taa\n"
    "2\tbb\n"
    "3\tcc\n"
    "4\taa\n"
    "5\tbb\n" == instance.query(
        f"SELECT * FROM {table_function} ORDER BY a SETTINGS allow_experimental_analyzer={new_analyzer}"
    )

    table_function_old = f"""
deltaLakeCluster(cluster_old,
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}',
        SETTINGS allow_experimental_delta_kernel_rs=1)
    """

    assert 5 == int(
        instance_old.query(
            f"SELECT count() FROM {table_function_old} SETTINGS allow_experimental_analyzer={new_analyzer}"
        )
    )

    # Incorrect result on old instance
    assert "1\n2\n3\n4\n5\n" == instance_old.query(
        f"SELECT * FROM {table_function_old} ORDER BY a SETTINGS allow_experimental_analyzer={new_analyzer}"
    )

    assert 5 == int(
        instance.query(
            f"SELECT count() FROM {table_function_old} SETTINGS allow_experimental_analyzer={new_analyzer}"
        )
    )

    # Incorrect result on old instance
    assert "1\t\\N\n2\t\\N\n3\t\\N\n4\t\\N\n5\t\\N\n" == instance.query(
        f"SELECT * FROM {table_function_old} ORDER BY a SETTINGS allow_experimental_analyzer={new_analyzer}"
    )


def test_partition_columns_3(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_partition_columns_3")
    result_file = f"{TABLE_NAME}"
    partition_columns = ["year"]

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("year", StringType(), nullable=False),
        ]
    )

    num_rows = 10
    now = datetime.now()
    data = [(i, f"name_{i}", 32, "US", "2025") for i in range(num_rows)]
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.write.mode("append").format("delta").partitionBy(partition_columns).save(
        f"/{TABLE_NAME}"
    )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    result = int(instance.query(f"SELECT count() FROM {table_function}"))
    assert result == num_rows

    assert (
        "0\tname_0\t32\tUS\t2025\n"
        "1\tname_1\t32\tUS\t2025\n"
        "2\tname_2\t32\tUS\t2025\n"
        "3\tname_3\t32\tUS\t2025\n"
        "4\tname_4\t32\tUS\t2025\n"
        "5\tname_5\t32\tUS\t2025\n"
        "6\tname_6\t32\tUS\t2025\n"
        "7\tname_7\t32\tUS\t2025\n"
        "8\tname_8\t32\tUS\t2025\n"
        "9\tname_9\t32\tUS\t2025"
        == instance.query(f"SELECT * FROM {table_function} ORDER BY all").strip()
    )


@pytest.mark.parametrize("use_delta_kernel", ["1", "0"])
def test_filtering_by_virtual_columns(started_cluster, use_delta_kernel):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_filtering_by_virtual_columns")
    result_file = f"{TABLE_NAME}"
    partition_columns = ["year"]

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("year", StringType(), nullable=False),
        ]
    )

    num_rows = 10
    now = datetime.now()
    data = [(i, f"name_{i}", 32, "US", f"202{i}") for i in range(num_rows)]
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.write.mode("append").format("delta").partitionBy(partition_columns).save(
        f"/{TABLE_NAME}"
    )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    result = int(
        instance.query(
            f"SELECT count() FROM {table_function} SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}"
        )
    )
    assert result == num_rows

    assert (
        "0\tname_0\t32\tUS\t2020\n"
        "1\tname_1\t32\tUS\t2021\n"
        "2\tname_2\t32\tUS\t2022\n"
        "3\tname_3\t32\tUS\t2023\n"
        "4\tname_4\t32\tUS\t2024\n"
        "5\tname_5\t32\tUS\t2025\n"
        "6\tname_6\t32\tUS\t2026\n"
        "7\tname_7\t32\tUS\t2027\n"
        "8\tname_8\t32\tUS\t2028\n"
        "9\tname_9\t32\tUS\t2029"
        == instance.query(
            f"SELECT * FROM {table_function} ORDER BY all SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}"
        ).strip()
    )

    query_id = f"query_{TABLE_NAME}_1"
    result = int(
        instance.query(
            f"SELECT count() FROM {table_function} WHERE _path ILIKE '%year=2024%' SETTINGS allow_experimental_delta_kernel_rs={use_delta_kernel}",
            query_id=query_id,
        )
    )
    assert result == 1
    instance.query("SYSTEM FLUSH LOGS")
    assert result == int(
        instance.query(
            f"SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id = '{query_id}' and type = 'QueryFinish'"
        )
    )

    if use_delta_kernel == "0":
        assert 1 < int(
            instance.query(
                f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and logger_name = 'DeltaLakeMetadataParser'"
            )
        )
    elif use_delta_kernel == "1":
        assert 0 == int(
            instance.query(
                f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and logger_name = 'DeltaLakeMetadataParser'"
            )
        )
    else:
        assert False


def test_column_pruning(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_column_pruning")
    result_file = f"{TABLE_NAME}"
    partition_columns = []

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("year", StringType(), nullable=False),
        ]
    )

    num_rows = 10000
    now = datetime.now()
    data = [
        (i, f"name_{i}", 32, "".join("a" for _ in range(100)), "2025")
        for i in range(num_rows)
    ]
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.write.mode("append").format("delta").partitionBy(partition_columns).save(
        f"/{TABLE_NAME}"
    )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    query_id = f"query_{TABLE_NAME}_1"
    sum = int(
        instance.query(
            f"SELECT sum(id) FROM {table_function} SETTINGS allow_experimental_delta_kernel_rs=0, max_read_buffer_size_remote_fs=100",
            query_id=query_id,
        )
    )
    instance.query("SYSTEM FLUSH LOGS")
    assert 107220 == int(
        instance.query(
            f"SELECT ProfileEvents['ReadBufferFromS3Bytes'] FROM system.query_log WHERE query_id = '{query_id}' and type = 'QueryFinish'"
        )
    )

    query_id = f"query_{TABLE_NAME}_2"
    assert sum == int(
        instance.query(
            f"SELECT sum(id) FROM {table_function} SETTINGS enable_filesystem_cache=0, max_read_buffer_size_remote_fs=100",
            query_id=query_id,
        )
    )
    instance.query("SYSTEM FLUSH LOGS")
    assert 1 == int(
        instance.query(
            f"SELECT ProfileEvents['EngineFileLikeReadFiles'] FROM system.query_log WHERE query_id = '{query_id}' and type = 'QueryFinish'"
        )
    )
    # Small diff because in case of delta-kernel metadata reading is not counted in the metric.
    assert 105677 == int(
        instance.query(
            f"SELECT ProfileEvents['ReadBufferFromS3Bytes'] FROM system.query_log WHERE query_id = '{query_id}' and type = 'QueryFinish'"
        )
    )


def test_concurrent_reads(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_concurrent_reads")
    result_file = f"{TABLE_NAME}"
    partition_columns = []

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("year", StringType(), nullable=False),
        ]
    )

    num_rows = 500000
    now = datetime.now()
    data = [
        (i, f"name_{i}", 32, "".join("a" for _ in range(100)), "2025")
        for i in range(num_rows)
    ]
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.write.mode("append").format("delta").partitionBy(partition_columns).save(
        f"/{TABLE_NAME}"
    )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")
    instance.query(
        f"create table {TABLE_NAME} (id Int32, name String, age Int32, country String, year String) engine = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    )

    def select(_):
        instance.query(
            f"SELECT * FROM {TABLE_NAME} SETTINGS max_read_buffer_size_remote_fs=100",
        )

    busy_pool = Pool(10)
    p = busy_pool.map_async(select, range(10))
    p.wait()

    select(0)


def test_snapshot_version(started_cluster):
    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_snapshot_version")
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    path = f"/{table_name}"
    column_mapping = "name"

    df = spark.createDataFrame([("alice", 47), ("anora", 23), ("aelin", 51)]).toDF(
        "first_name", "age"
    )

    df.write.format("delta").partitionBy("age").option(
        "delta.minReaderVersion", "2"
    ).option("delta.minWriterVersion", "5").option(
        "delta.columnMapping.mode", column_mapping
    ).save(
        path
    )

    upload_directory(minio_client, bucket, path, "")

    delta_function = f"""
deltaLake(
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}')
    """

    def check_schema(expected, version):
        assert (
            expected
            == node.query(
                f"DESCRIBE TABLE {delta_function} SETTINGS delta_lake_snapshot_version = {version}"
            ).strip()
        )

    def check_data(expected, version):
        assert (
            expected
            == node.query(
                f"SELECT * FROM {delta_function} ORDER BY all SETTINGS delta_lake_snapshot_version = {version}"
            ).strip()
        )

    def append_data(df):
        df.write.option("mergeSchema", "true").mode("append").format(
            "delta"
        ).partitionBy("age").save(path)
        upload_directory(minio_client, bucket, path, "")

    check_schema("first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)", 0)
    check_data("aelin\t51\n" "alice\t47\n" "anora\t23", 0)

    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")
    spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN first_name TO naam")

    df = spark.createDataFrame([("bob", 12), ("bill", 33), ("bober", 49)]).toDF(
        "naam", "age"
    )
    append_data(df)

    check_schema("first_name\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)", 0)
    check_data("aelin\t51\n" "alice\t47\n" "anora\t23", 0)

    check_schema("naam\tNullable(String)\t\t\t\t\t\nage\tNullable(Int64)", 2)
    check_data("aelin\t51\nalice\t47\nanora\t23\nbill\t33\nbob\t12\nbober\t49", 2)

    assert "Unknown expression identifier `first_name`" in node.query_and_get_error(
        f"SELECT first_name FROM {delta_function} WHERE age = 51"
    )

    assert "Unknown expression identifier `first_name`" in node.query_and_get_error(
        f"SELECT first_name FROM {delta_function} WHERE age = 51",
        settings={"delta_lake_snapshot_version": 2},
    )

    assert (
        "aelin"
        in node.query(
            f"SELECT first_name FROM {delta_function} WHERE age = 51",
            settings={"delta_lake_snapshot_version": 0},
        ).strip()
    )

    assert "Unknown expression identifier `naam`" in node.query_and_get_error(
        f"SELECT naam FROM {delta_function} WHERE age = 51",
        settings={"delta_lake_snapshot_version": 0},
    )

    assert (
        "aelin"
        in node.query(
            f"SELECT naam FROM {delta_function} WHERE age = 51",
            settings={"delta_lake_snapshot_version": 2},
        ).strip()
    )


def test_join_with_distributed(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = randomize_table_name("test_join_with_distributed")
    result_file = f"{TABLE_NAME}"

    df = spark.createDataFrame(
        [
            (1, "a"),
            (2, "b"),
            (3, "c"),
            (4, "d"),
            (5, "e"),
            (6, "f"),
            (7, "g"),
            (8, "h"),
            (9, "i"),
        ],
        ["id", "val"],
    )

    df.write.format("delta").save(f"/{TABLE_NAME}")

    clickhouse_table_name = f"test_join_with_distributed_{uuid.uuid4().hex}"

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    instance.query(
        f"create table {clickhouse_table_name} on cluster cluster (id UInt8, val char) engine = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{clickhouse_table_name}', '{{replica}}') order by id"
    )
    instance.query(
        f"create table {clickhouse_table_name}_dist on cluster cluster AS {clickhouse_table_name} engine = Distributed(cluster, default, {clickhouse_table_name}, rand())"
    )
    instance.query(
        f"insert into {clickhouse_table_name}_dist values (1, 'A'),(2, 'B'),(3, 'C'),(4, 'D'),(5, 'E'),(6, 'F'),(7, 'G'),(8, 'H'),(9, 'I');"
    )

    table_function_cluster = f"deltaLakeCluster(cluster, 'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    # All cases which were reproted as faulty
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {table_function_cluster} SETTINGS prefer_localhost_replica = 0"
            ).strip()
        )
        == 9
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM {table_function} SETTINGS cluster_for_parallel_replicas='cluster', max_parallel_replicas=2, allow_experimental_parallel_reading_from_replicas=2, parallel_replicas_for_cluster_engines=1"
            ).strip()
        )
        == 9
    )

    assert (
        len(
            instance.query(
                f"with b as (select * from {table_function}) select {clickhouse_table_name}_dist.val, b.val from {clickhouse_table_name}_dist join b on {clickhouse_table_name}_dist.id = b.id;"
            ).split("\n")
        )
        == 10
    )
    assert (
        len(
            instance.query(
                f"with b as (select * from {table_function}) select {clickhouse_table_name}_dist.val, b.val from b join {clickhouse_table_name}_dist on {clickhouse_table_name}_dist.id = b.id;"
            ).split("\n")
        )
        == 10
    )
    assert (
        int(
            instance.query(
                f"SELECT count() FROM remote('localhost', {table_function}) SETTINGS prefer_localhost_replica = 0"
            ).strip()
        )
        == 9
    )


def test_delta_kernel_internal_pruning(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_partition_columns")
    result_file = f"{TABLE_NAME}"
    partition_columns = ["b", "c", "d", "e", "f", "g", "h"]

    delta_table = (
        DeltaTable.create(spark)
        .tableName(TABLE_NAME)
        .location(f"/{result_file}")
        .addColumn("a", "INT")
        .addColumn("b", "STRING")
        .addColumn("c", "DATE")
        .addColumn("d", "INT")
        .addColumn("e", "TIMESTAMP")
        .addColumn("f", "BOOLEAN")
        .addColumn("g", "DECIMAL(10,2)")
        .addColumn("h", "BOOLEAN")
        .partitionedBy(partition_columns)
        .execute()
    )
    num_rows = 9

    schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DateType()),
            StructField("d", IntegerType()),
            StructField("e", TimestampType()),
            StructField("f", BooleanType()),
            StructField("g", DecimalType(10, 2)),
            StructField("h", BooleanType()),
        ]
    )

    now = datetime.now()
    for i in range(1, num_rows + 1):
        data = [
            (
                i,
                "test" + str(i % 3),
                datetime.strptime(f"2000-01-0{i}", "%Y-%m-%d"),
                i % 2,
                (
                    now
                    if i % 2 == 0
                    else datetime.strptime(
                        f"2012-01-0{i} 12:34:56.789123", "%Y-%m-%d %H:%M:%S.%f"
                    )
                ),
                True if i % 2 == 0 else False,
                Decimal(f"{i * 1.11:.2f}"),
                False if i % 2 == 0 else True,
            )
        ]
        df = spark.createDataFrame(data=data, schema=schema)
        df.printSchema()
        df.write.mode("append").format("delta").partitionBy(partition_columns).save(
            f"/{TABLE_NAME}"
        )

    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket

    files = upload_directory(minio_client, bucket, f"/{TABLE_NAME}", "")
    assert len(files) > 0
    print(f"Uploaded files: {files}")

    cluster = False
    if cluster:
        table_function = f"deltaLakeCluster(cluster, 'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    else:
        table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    result = instance.query(f"describe table {table_function}").strip()
    assert (
        result == "a\tNullable(Int32)\t\t\t\t\t\n"
        "b\tNullable(String)\t\t\t\t\t\n"
        "c\tNullable(Date32)\t\t\t\t\t\n"
        "d\tNullable(Int32)\t\t\t\t\t\n"
        "e\tNullable(DateTime64(6))\t\t\t\t\t\n"
        "f\tNullable(Bool)\t\t\t\t\t\n"
        "g\tNullable(Decimal(10, 2))\t\t\t\t\t\n"
        "h\tNullable(Bool)"
    )

    result = int(instance.query(f"SELECT count() FROM {table_function}"))
    assert result == num_rows

    expected_output = f"""1	test1	2000-01-01	1	2012-01-01 12:34:56.789123	false	1.11	true
2	test2	2000-01-02	0	{now}	true	2.22	false
3	test0	2000-01-03	1	2012-01-03 12:34:56.789123	false	3.33	true
4	test1	2000-01-04	0	{now}	true	4.44	false
5	test2	2000-01-05	1	2012-01-05 12:34:56.789123	false	5.55	true
6	test0	2000-01-06	0	{now}	true	6.66	false
7	test1	2000-01-07	1	2012-01-07 12:34:56.789123	false	7.77	true
8	test2	2000-01-08	0	{now}	true	8.88	false
9	test0	2000-01-09	1	2012-01-09 12:34:56.789123	false	9.99	true"""

    assert (
        expected_output
        == instance.query(f"SELECT * FROM {table_function} ORDER BY a").strip()
    )

    query_id = f"query_with_filter_{TABLE_NAME}_1"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE 'test2' == b
            """,
            query_id=query_id,
        )
    )
    assert result == 3
    instance.query("SYSTEM FLUSH LOGS")
    assert 3 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 3 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_2"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE 'test2' == b AND d == 1
            """,
            query_id=query_id,
        )
    )
    assert result == 1
    instance.query("SYSTEM FLUSH LOGS")
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_3"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE 'test2' == b AND d != 1
            """,
            query_id=query_id,
        )
    )

    assert result == 2
    instance.query("SYSTEM FLUSH LOGS")
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_4"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE 'test2' == b AND d > 0
            """,
            query_id=query_id,
        )
    )

    assert result == 1
    instance.query("SYSTEM FLUSH LOGS")
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_5"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE b == 'test2' AND d < 1
            """,
            query_id=query_id,
        )
    )

    assert result == 2
    instance.query("SYSTEM FLUSH LOGS")
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_6"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE b == 'test2' AND 1 >= d
            """,
            query_id=query_id,
        )
    )

    assert result == 1
    instance.query("SYSTEM FLUSH LOGS")
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_7"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE b == 'test2' AND d <= 0
            """,
            query_id=query_id,
        )
    )

    assert result == 2
    instance.query("SYSTEM FLUSH LOGS")
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_8"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE c == toDate('2000-01-08')
            """,
            query_id=query_id,
        )
    )

    assert result == 1
    instance.query("SYSTEM FLUSH LOGS")
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_9"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE c == '2000-01-08'
            """,
            query_id=query_id,
        )
    )

    assert result == 1
    instance.query("SYSTEM FLUSH LOGS")
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 1 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_10"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE b == 'test2' AND not (h == 1)
            """,
            query_id=query_id,
        )
    )

    assert result == 2
    instance.query("SYSTEM FLUSH LOGS")
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    query_id = f"query_with_filter_{TABLE_NAME}_11"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE b == 'test2' AND not h
            """,
            query_id=query_id,
        )
    )

    assert result == 2
    instance.query("SYSTEM FLUSH LOGS")
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )
    assert 2 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file: {TABLE_NAME}/b=test2%'"
        )
    )

    instance.query("SYSTEM ENABLE FAILPOINT delta_kernel_fail_literal_visitor")

    query_id = f"query_with_filter_{TABLE_NAME}_12"
    assert "Injecting fault for visitLiteralValue" in instance.query_and_get_error(
        f"""SELECT count() FROM {table_function} WHERE b == 'test2'
        """,
        query_id=query_id,
        settings={"delta_lake_throw_on_engine_predicate_error": 1},
    )

    query_id = f"query_with_filter_{TABLE_NAME}_13"
    result = int(
        instance.query(
            f"""SELECT count() FROM {table_function} WHERE b == 'test2'
        """,
            query_id=query_id,
        )
    )

    instance.query("SYSTEM DISABLE FAILPOINT delta_kernel_fail_literal_visitor")

    assert result == 3
    instance.query("SYSTEM FLUSH LOGS")
    assert 3 == int(
        instance.query(
            f"SELECT count() FROM system.text_log WHERE query_id = '{query_id}' and message ILIKE '%Scanned file%'"
        )
    )


def test_count_from_cache(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_empty_format_header")
    result_file = f"{TABLE_NAME}"

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ]
    )
    df = spark.createDataFrame([(1, "keko"), (2, "puka"), (3, "mora")]).toDF(
        "id", "name"
    )
    df.write.format("delta").partitionBy("id").save(f"/{result_file}")
    upload_directory(minio_client, bucket, f"/{result_file}", "")

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    time.sleep(1)
    assert 3 == int(instance.query(f"SELECT count() FROM {table_function}"))
    assert 3 == int(instance.query(f"SELECT count() FROM {table_function}"))
    query_id = f"{TABLE_NAME}_query"
    assert 3 == int(
        instance.query(f"SELECT count() FROM {table_function}", query_id=query_id)
    )
    instance.query("SYSTEM FLUSH LOGS")
    assert 3 == int(
        instance.query(
            f"SELECT ProfileEvents['SchemaInferenceCacheNumRowsHits'] FROM system.query_log WHERE query_id = '{query_id}' and type = 'QueryFinish'"
        )
    )
    assert (
        "3\t3"
        == instance.query(f"SELECT count(), count() FROM {table_function}").strip()
    )
    assert (
        "3\t6\t3"
        == instance.query(
            f"SELECT count(), sum(id), uniqExact(_path) FROM {table_function}"
        ).strip()
    )


def test_writes(started_cluster):
    instance = started_cluster.instances["node1"]
    instance_disabled_kernel = cluster.instances["node_with_disabled_delta_kernel"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    table_name = randomize_table_name("test_writes")
    result_file = f"{table_name}_data"

    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    empty_arrays = [pa.array([], type=pa.int32()), pa.array([], type=pa.string())]
    write_deltalake(
        f"s3://root/{result_file}",
        pa.Table.from_arrays(empty_arrays, schema=schema),
        storage_options=get_storage_options(started_cluster),
        mode="overwrite",
    )

    instance.query(
        f"CREATE TABLE {table_name} (id Int32, name String) ENGINE = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    )
    instance_disabled_kernel.query(
        f"CREATE TABLE {table_name} (id Int32, name String) ENGINE = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    )
    instance.query(
        f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers(10)"
    )

    s3_objects = list(minio_client.list_objects(bucket, result_file, recursive=True))
    file_name = None
    for obj in s3_objects:
        print(f"File: {obj.object_name}")
        if obj.object_name.endswith("parquet") and not obj.object_name.endswith(
            "snappy.parquet"
        ):
            file_name = obj.object_name

    assert (
        "0\t0\n1\t1\n2\t2\n3\t3\n4\t4\n5\t5\n6\t6\n7\t7\n8\t8\n9\t9"
        == instance.query(
            f"SELECT * FROM s3('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{file_name}', 'minio', '{minio_secret_key}')"
        ).strip()
    )

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    def check_count(expected):
        assert expected == int(instance.query(f"SELECT count() FROM {table_function}"))
        assert expected == int(
            instance_disabled_kernel.query(f"SELECT count() FROM {table_name}")
        )
        assert expected == int(instance.query(f"SELECT count() FROM {table_name}"))

    def check_data(expected):
        assert (
            expected
            == instance.query(f"SELECT * FROM {table_name} ORDER BY all").strip()
        )
        assert (
            expected
            == instance_disabled_kernel.query(
                f"SELECT * FROM {table_name} ORDER BY all"
            ).strip()
        )
        assert (
            expected
            == instance.query(f"SELECT * FROM {table_function} ORDER BY all").strip()
        )

    check_count(10)
    check_data("0\t0\n1\t1\n2\t2\n3\t3\n4\t4\n5\t5\n6\t6\n7\t7\n8\t8\n9\t9")

    instance.query(
        f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers(10, 10)"
    )
    check_count(20)
    check_data(
        "0\t0\n1\t1\n2\t2\n3\t3\n4\t4\n5\t5\n6\t6\n7\t7\n8\t8\n9\t9\n10\t10\n11\t11\n12\t12\n13\t13\n14\t14\n15\t15\n16\t16\n17\t17\n18\t18\n19\t19"
    )


def test_partitioned_writes(started_cluster):
    instance = started_cluster.instances["node1"]
    instance_disabled_kernel = cluster.instances["node_with_disabled_delta_kernel"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    table_name = randomize_table_name("test_partitioned_writes")
    result_file = f"{table_name}_data"
    partition_columns = ["id", "comment"]

    schema = pa.schema(
        [("id", pa.int32()), ("name", pa.string()), ("comment", pa.string())]
    )
    empty_arrays = [
        pa.array([], type=pa.int32()),
        pa.array([], type=pa.string()),
        pa.array([], type=pa.string()),
    ]
    write_deltalake(
        f"s3://root/{result_file}",
        pa.Table.from_arrays(empty_arrays, schema=schema),
        storage_options=get_storage_options(started_cluster),
        mode="overwrite",
        partition_by=partition_columns,
    )

    instance.query(
        f"CREATE TABLE {table_name} (id Int32, name String, comment String) ENGINE = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    )
    instance_disabled_kernel.query(
        f"CREATE TABLE {table_name} (id Int32, name String, comment String) ENGINE = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    )
    instance.query(
        f"INSERT INTO {table_name} SELECT number, toString(number), concat('comment-', toString(number % 2)) FROM numbers(10)"
    )

    def check_files(expected):
        s3_objects = list(
            minio_client.list_objects(bucket, result_file, recursive=True)
        )
        file_names = []
        for obj in s3_objects:
            print(f"File: {obj.object_name}")
            if obj.object_name.endswith("parquet") and not obj.object_name.endswith(
                "snappy.parquet"
            ):
                file_names.append(obj.object_name)

        assert len(file_names) == expected
        file_names.sort()
        expected_ids = [str(i) for i in range(expected)]
        expected_ids.sort()
        for i in range(expected):
            expected_id = expected_ids[i]
            comment_id = int(expected_ids[i]) % 2
            assert file_names[i].startswith(
                f"{result_file}/id={expected_id}/comment=comment-{comment_id}/"
            )
            assert (
                f"{expected_id}\t{expected_id}\tcomment-{comment_id}"
                == instance.query(
                    f"SELECT id, name, comment FROM s3('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{file_names[i]}', 'minio', '{minio_secret_key}') ORDER BY all"
                ).strip()
            )
            assert (
                f"{expected_id}"
                == instance.query(
                    f"SELECT * FROM s3('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{file_names[i]}', 'minio', '{minio_secret_key}')",
                    settings={"use_hive_partitioning": 0},
                ).strip()
            )

    table_function = f"deltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"

    def check_count(expected):
        assert expected == int(
            instance.query(
                f"SELECT count() FROM {table_name} settings use_cache_for_count_from_files=0"
            )
        )
        assert expected == int(
            instance.query(
                f"SELECT count() FROM {table_function} settings use_cache_for_count_from_files=0"
            )
        )
        assert expected == int(
            instance_disabled_kernel.query(
                f"SELECT count() FROM {table_name} settings use_cache_for_count_from_files=0"
            )
        )

    def check_data(expected):
        assert (
            expected
            == instance.query(f"SELECT * FROM {table_name} ORDER BY all").strip()
        )
        # TODO: Fix read with disabled delta-kernel?
        # assert (
        #    expected
        #    == instance_disabled_kernel.query(
        #        f"SELECT * FROM {table_name} ORDER BY all"
        #    ).strip()
        # )
        assert (
            expected
            == instance.query(f"SELECT * FROM {table_function} ORDER BY all").strip()
        )

    check_files(10)
    check_count(10)
    check_data(
        "0\t0\tcomment-0\n1\t1\tcomment-1\n2\t2\tcomment-0\n3\t3\tcomment-1\n4\t4\tcomment-0\n5\t5\tcomment-1\n6\t6\tcomment-0\n7\t7\tcomment-1\n8\t8\tcomment-0\n9\t9\tcomment-1"
    )

    instance.query(
        f"INSERT INTO {table_name} SELECT number, toString(number), concat('comment-', toString(number % 2))  FROM numbers(10, 10)"
    )
    check_count(20)
    check_files(20)
    check_data(
        "0\t0\tcomment-0\n1\t1\tcomment-1\n2\t2\tcomment-0\n3\t3\tcomment-1\n4\t4\tcomment-0\n5\t5\tcomment-1\n6\t6\tcomment-0\n7\t7\tcomment-1\n8\t8\tcomment-0\n9\t9\tcomment-1\n10\t10\tcomment-0\n11\t11\tcomment-1\n12\t12\tcomment-0\n13\t13\tcomment-1\n14\t14\tcomment-0\n15\t15\tcomment-1\n16\t16\tcomment-0\n17\t17\tcomment-1\n18\t18\tcomment-0\n19\t19\tcomment-1"
    )


@pytest.mark.parametrize("partitioned", [False, True])
def test_concurrent_queries(started_cluster, partitioned):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    TABLE_NAME = randomize_table_name("test_concurrent_queries")
    result_file = f"{TABLE_NAME}"

    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    empty_arrays = [pa.array([], type=pa.int32()), pa.array([], type=pa.string())]
    write_deltalake(
        f"s3://root/{result_file}",
        pa.Table.from_arrays(empty_arrays, schema=schema),
        storage_options=get_storage_options(started_cluster),
        mode="overwrite",
        partition_by=["name"] if partitioned else [],
    )

    instance.query(
        f"create table {TABLE_NAME} (id Int32, name String) engine = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{result_file}/', 'minio', '{minio_secret_key}')"
    )

    def select(_):
        instance.query(f"SELECT * FROM {TABLE_NAME}")

    num_threads = 10
    num_rows = 50
    errors = ["" for _ in range(num_threads)]
    success = [0 for _ in range(num_threads)]

    def insert(i):
        try:
            instance.query(
                f"INSERT INTO {TABLE_NAME} SELECT number, toString(number) FROM numbers({num_rows})",
            )
            success[i] += 1
        except Exception as e:
            errors[i] = str(e)

    for _ in range(10):
        insert(_)

    select_pool = Pool(num_threads)
    insert_pool = Pool(num_threads)
    sp = select_pool.map_async(select, range(num_threads))
    ip = insert_pool.map_async(insert, range(num_threads))
    sp.wait()
    ip.wait()

    select(0)

    assert sum(success) * num_rows == int(
        instance.query(
            f"SELECT count() FROM {TABLE_NAME}",
        )
    )
    non_empty_errors = [e for e in errors if e != ""]
    assert len(non_empty_errors) > 0
    for e in non_empty_errors:
        assert "commit conflict at version" in e

    s3_objects = list(minio_client.list_objects(bucket, result_file, recursive=True))
    file_names = []
    for obj in s3_objects:
        print(f"File: {obj.object_name}")
        if obj.object_name.endswith("parquet") and not obj.object_name.endswith(
            "snappy.parquet"
        ):
            file_names.append(obj.object_name)
    if partitioned:
        assert len(file_names) == sum(success) * num_rows
    else:
        assert len(file_names) == sum(success)


def test_writes_spark_compatibility(started_cluster):
    instance = started_cluster.instances["node1"]
    instance_disabled_kernel = cluster.instances["node_with_disabled_delta_kernel"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    table_name = randomize_table_name("test_writes")
    result_file = f"{table_name}_data"

    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    empty_arrays = [pa.array([], type=pa.int32()), pa.array([], type=pa.string())]
    write_deltalake(
        f"file:///{result_file}",
        pa.Table.from_arrays(empty_arrays, schema=schema),
        mode="overwrite",
    )

    LocalUploader(instance).upload_directory(f"/{result_file}/", f"/{result_file}/")
    files = (
        instance.exec_in_container(["bash", "-c", f"ls /{result_file}"])
        .strip()
        .split("\n")
    )
    assert len(files) == 1
    assert "_delta_log" == files[0]
    assert "" in instance.exec_in_container(
        ["bash", "-c", f"ls /{result_file}/_delta_log"]
    )

    instance.query(
        f"CREATE TABLE {table_name} (id Int32, name String) ENGINE = DeltaLakeLocal('/{result_file}') SETTINGS output_format_parquet_compression_method = 'none'"
    )
    instance.query(
        f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers(10)"
    )

    LocalDownloader(instance).download_directory(f"/{result_file}/", f"/{result_file}/")

    files = (
        instance.exec_in_container(["bash", "-c", f"ls /{result_file}"])
        .strip()
        .split("\n")
    )
    assert len(files) == 2
    pfile = files[0] if files[0].endswith(".parquet") else files[1]

    table = pq.read_table(f"/{result_file}/{pfile}")
    df = table.to_pandas()
    assert (
        "0   0    0\n1   1    1\n2   2    2\n3   3    3\n4   4    4\n5   5    5\n6   6    6\n7   7    7\n8   8    8\n9   9    9"
        in str(df)
    )

    spark = started_cluster.spark_session
    df = spark.read.format("delta").load(f"/{result_file}").collect()
    assert (
        "[Row(id=0, name='0'), Row(id=1, name='1'), Row(id=2, name='2'), Row(id=3, name='3'), Row(id=4, name='4'), Row(id=5, name='5'), Row(id=6, name='6'), Row(id=7, name='7'), Row(id=8, name='8'), Row(id=9, name='9')]"
        == str(df)
    )

    instance.query(
        f"INSERT INTO {table_name} SELECT number, toString(number) FROM numbers(10, 10)"
    )
    LocalDownloader(instance).download_directory(f"/{result_file}/", f"/{result_file}/")
    files = (
        instance.exec_in_container(["bash", "-c", f"ls /{result_file}"])
        .strip()
        .split("\n")
    )
    assert len(files) == 3

    df = spark.read.format("delta").load(f"/{result_file}").collect()
    assert (
        "[Row(id=10, name='10'), Row(id=11, name='11'), Row(id=12, name='12'), Row(id=13, name='13'), Row(id=14, name='14'), Row(id=15, name='15'), Row(id=16, name='16'), Row(id=17, name='17'), Row(id=18, name='18'), Row(id=19, name='19'), Row(id=0, name='0'), Row(id=1, name='1'), Row(id=2, name='2'), Row(id=3, name='3'), Row(id=4, name='4'), Row(id=5, name='5'), Row(id=6, name='6'), Row(id=7, name='7'), Row(id=8, name='8'), Row(id=9, name='9')]"
        == str(df)
    )


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("limit_enabled", [False, True])
def test_write_limits(started_cluster, partitioned, limit_enabled):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    table_name = randomize_table_name("test_write_limits")
    result_file = f"{table_name}_data"

    schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
    empty_arrays = [pa.array([], type=pa.int32()), pa.array([], type=pa.string())]
    write_deltalake(
        f"file:///{result_file}",
        pa.Table.from_arrays(empty_arrays, schema=schema),
        mode="overwrite",
        partition_by=["id"] if partitioned else [],
    )
    LocalUploader(instance).upload_directory(f"/{result_file}/", f"/{result_file}/")
    files = (
        instance.exec_in_container(["bash", "-c", f"ls /{result_file}"])
        .strip()
        .split("\n")
    )
    assert len(files) == 1

    instance.query(
        f"CREATE TABLE {table_name} (id Int32, name String) ENGINE = DeltaLakeLocal('/{result_file}') SETTINGS output_format_parquet_compression_method = 'none'"
    )

    num_rows = 1000000
    partitions_num = 5
    limit_rows = 10 if limit_enabled else (num_rows + 1)
    instance.query(
        f"INSERT INTO {table_name} SELECT number % {partitions_num}, randomString(10) FROM numbers({num_rows}) SETTINGS delta_lake_insert_max_rows_in_data_file = {limit_rows}, max_insert_block_size = 1000, min_chunk_bytes_for_parallel_parsing = 1000"
    )

    files = LocalDownloader(instance).download_directory(f"/{result_file}/", f"/{result_file}/")
    data_files = [file for file in files if file.endswith(".parquet")]
    assert len(data_files) > 0, f"No data files: {files}"

    if partitioned:
        if limit_enabled:
            assert len(data_files) > partitions_num, f"Data files: {data_files}"
        else:
            assert len(data_files) == partitions_num, f"Data files: {data_files}"
    else:
        if limit_enabled:
            assert len(data_files) > 1, f"Data files: {data_files}"
        else:
            assert len(data_files) == 1, f"Data files: {data_files}"

    assert num_rows == int(instance.query(f"SELECT count() FROM {table_name}"))

    spark = started_cluster.spark_session
    df = spark.read.format("delta").load(f"/{result_file}")
    assert df.count() == num_rows


def test_column_mapping_id(started_cluster):
    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_column_mapping_id")
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    path = f"/{table_name}"

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField(
                "person",
                StructType(
                    [
                        StructField("first_name", StringType(), True),
                        StructField("last_name", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )
    data = [(1, ("Alice", "Smith")), (2, ("Bob", "Johnson"))]
    df = spark.createDataFrame(data, schema=schema)
    df.write.format("delta").option("delta.minReaderVersion", "2").option(
        "delta.minWriterVersion", "5"
    ).option("delta.columnMapping.mode", "id").save(path)
    upload_directory(minio_client, bucket, path, "")

    delta_function = f"""
deltaLake(
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}')
    """
    assert (
        "Column mapping ID mode not supported"
        in node.query_and_get_error(
            f"SELECT * FROM {delta_function} ORDER BY all"
        ).strip()
    )


@pytest.mark.parametrize("column_mapping", ["", "name"])
def test_subcolumns(started_cluster, column_mapping):
    node = started_cluster.instances["node1"]
    table_name = randomize_table_name("test_struct")
    spark = started_cluster.spark_session
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    path = f"/{table_name}"

    data_file = "field_ids_struct_test/data/00000-1-7cad83a6-af90-42a9-8a10-114cbc862a42-0-00001.parquet"

    def s3_function(path):
        return f""" s3(
            'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/{bucket}/{path}' ,
            '{minio_access_key}',
            '{minio_secret_key}')
        """

    func = s3_function(data_file)
    assert (
        "2025-06-04\t('100022','2025-06-04 18:40:56.000000','2025-06-09 21:19:00.364000')\t100022"
        == node.query(f"select * from {func}").strip()
    )
    assert (
        "col_x2D1\tNullable(Date32)\t\t\t\t\t\n"
        "col_x2D2\tTuple(\\n    col_x2D3 Nullable(String),\\n    col_x2D4 Nullable(DateTime64(6, \\'UTC\\')),\\n    col_x2D5 Nullable(DateTime64(6, \\'UTC\\')))\t\t\t\t\t\n"
        "col_x2D6\tNullable(Int64)" == node.query(f"describe table {func}").strip()
    )

    df = spark.read.parquet(os.path.join(SCRIPT_DIR, data_file))
    write_delta_from_df(spark, df, path, mode="overwrite")
    default_upload_directory(started_cluster, "s3", path, "")

    s3_objects = list(minio_client.list_objects(bucket, table_name, recursive=True))
    file_names = []
    object_name = None
    for obj in s3_objects:
        print(f"File: {obj.object_name}")
        if obj.object_name.endswith(".parquet"):
            object_name = obj.object_name

    func = s3_function(object_name)
    assert (
        "2025-06-04\t('100022','2025-06-04 18:40:56.000000000','2025-06-09 21:19:00.364000000')\t100022"
        == node.query(f"select * from {func}").strip()
    )
    data_file_desc = node.query(f"describe table {func}").strip()
    assert "col-" in data_file_desc
    assert "col_" not in data_file_desc

    data_file_schema = node.query(f"describe table {func}")
    print(f"Data file schema: {data_file_schema}")

    delta_function = f"""
deltaLake(
        'http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}')
    """

    assert (
        "2025-06-04\t('100022','2025-06-04 18:40:56.000000','2025-06-09 21:19:00.364000')\t100022"
        == node.query(f"SELECT * FROM {delta_function} ORDER BY all").strip()
    )

    assert (
        "col_x2D1\tNullable(Date32)\t\t\t\t\t\n"
        "col_x2D2\tTuple(\\n    col_x2D3 Nullable(String),\\n    col_x2D4 Nullable(DateTime64(6)),\\n    col_x2D5 Nullable(DateTime64(6)))\t\t\t\t\t\n"
        "col_x2D6\tNullable(Int64)"
        == node.query(f"describe table {delta_function}").strip()
    )

    node.query(
        f"""
    CREATE TABLE {table_name} (
    col_x2D1 Nullable(Date32),
    col_x2D2 Tuple(col_x2D3 Nullable(String), col_x2D4 Nullable(DateTime64(6)), col_x2D5 Nullable(DateTime64(6))),
    col_x2D6 Nullable(Int64)) ENGINE = DeltaLake('http://{started_cluster.minio_ip}:{started_cluster.minio_port}/root/{table_name}' ,
        '{minio_access_key}',
        '{minio_secret_key}')
    """
    )

    assert (
        "2025-06-04\t('100022','2025-06-04 18:40:56.000000','2025-06-09 21:19:00.364000')\t100022"
        == node.query(f"SELECT * FROM {table_name} ORDER BY all").strip()
    )


def test_write_column_order(started_cluster):
    instance = started_cluster.instances["node1"]
    minio_client = started_cluster.minio_client
    bucket = started_cluster.minio_bucket
    table_name = randomize_table_name("test_write_column_order")
    result_file = f"{table_name}_data"
    schema = pa.schema([("c1", pa.int32()), ("c0", pa.string())])
    empty_arrays = [pa.array([], type=pa.int32()), pa.array([], type=pa.string())]
    write_deltalake(
        f"file:///{result_file}",
        pa.Table.from_arrays(empty_arrays, schema=schema),
        mode="overwrite",
    )
    LocalUploader(instance).upload_directory(f"/{result_file}/", f"/{result_file}/")

    instance.query(
        f"CREATE TABLE {table_name} (c0 String, c1 Int32) ENGINE = DeltaLakeLocal('/{result_file}') SETTINGS output_format_parquet_compression_method = 'none'"
    )
    num_rows = 10
    instance.query(
        f"INSERT INTO {table_name} (c1, c0) SELECT number as c1, toString(number % 2) as c0 FROM numbers(10)"
    )

    assert num_rows == int(instance.query(f"SELECT count() FROM {table_name}"))
    assert (
        "0\t0\n1\t1\n0\t2\n1\t3\n0\t4\n1\t5\n0\t6\n1\t7\n0\t8\n1\t9"
        == instance.query(f"SELECT c0, c1 FROM {table_name}").strip()
    )

    instance.query(
        f"INSERT INTO {table_name} (c1, c0) SELECT c1, c0 FROM generateRandom('c1 Int32, c0 String', 16920040705558589162, 7706, 3) LIMIT {num_rows}"
    )

    assert num_rows * 2 == int(instance.query(f"SELECT count() FROM {table_name}"))
