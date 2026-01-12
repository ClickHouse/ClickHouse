import logging
import os
import uuid

import pyspark
import pytest
import pandas as pd

from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    StringType,
)
from pyspark.sql.window import Window

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    S3Downloader,
    prepare_s3_bucket,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("spark_test")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/var/lib/clickhouse/user_files/iceberg_data")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )
    return builder.master("local").getOrCreate()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
                "configs/config.d/filesystem_caches.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = get_spark()
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.azure_container_name = "mycontainer"

        cluster.blob_service_client = cluster.blob_service_client

        container_client = cluster.blob_service_client.create_container(
            cluster.azure_container_name
        )

        cluster.container_client = container_client

        cluster.default_azure_uploader = AzureUploader(
            cluster.blob_service_client, cluster.azure_container_name
        )

        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
        cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])
        cluster.default_s3_downloader = S3Downloader(cluster.minio_client, cluster.minio_bucket)

        yield cluster

    finally:
        cluster.shutdown()


def run_query(instance, query, stdin=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    result = instance.query(query, stdin=stdin, settings=settings)
    logging.info("Query finished")

    return result


def write_iceberg_from_file(
    spark, path, table_name, mode="overwrite", format_version="1", partition_by=None
):
    if mode == "overwrite":
        if partition_by is None:
            spark.read.load(f"file://{path}").writeTo(table_name).tableProperty(
                "format-version", format_version
            ).using("iceberg").create()
        else:
            spark.read.load(f"file://{path}").writeTo(table_name).partitionedBy(
                partition_by
            ).tableProperty("format-version", format_version).using("iceberg").create()
    else:
        spark.read.load(f"file://{path}").writeTo(table_name).append()


def write_iceberg_from_df(
    spark, df, table_name, mode="overwrite", format_version="1", partition_by=None
):
    if mode == "overwrite":
        if partition_by is None:
            df.writeTo(table_name).tableProperty(
                "format-version", format_version
            ).using("iceberg").create()
        else:
            df.writeTo(table_name).tableProperty(
                "format-version", format_version
            ).partitionedBy(partition_by).using("iceberg").create()
    else:
        df.writeTo(table_name).append()


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


def get_creation_expression(
    storage_type,
    table_name,
    cluster,
    schema="",
    format_version=2,
    partition_by="",
    if_not_exists=False,
    compression_method=None,
    format="Parquet",
    table_function=False,
    use_version_hint=False,
    run_on_cluster=False,
    explicit_metadata_path="",
    additional_settings = [],
    **kwargs,
):
    settings_array = list(additional_settings)

    if explicit_metadata_path:
        settings_array.append(f"iceberg_metadata_file_path = '{explicit_metadata_path}'")

    if use_version_hint:
        settings_array.append("iceberg_use_version_hint = true")

    if partition_by:
        partition_by = "PARTITION BY " + partition_by
    settings_array.append(f"iceberg_format_version = {format_version}")

    if compression_method:
        settings_array.append(f"iceberg_metadata_compression_method = '{compression_method}'")

    if settings_array:
        settings_expression = " SETTINGS " + ",".join(settings_array)
    else:
        settings_expression = ""

    if_not_exists_prefix = ""
    if if_not_exists:
        if_not_exists_prefix = "IF NOT EXISTS"

    if storage_type == "s3":
        if "bucket" in kwargs:
            bucket = kwargs["bucket"]
        else:
            bucket = cluster.minio_bucket

        if run_on_cluster:
            assert table_function
            return f"icebergS3Cluster('cluster_simple', s3, filename = 'var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"
        else:
            if table_function:
                return f"icebergS3(s3, filename = 'var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"
            else:
                return (
                    f"""
                    DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {if_not_exists_prefix} {table_name} {schema}
                    ENGINE=IcebergS3(s3, filename = 'var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')
                    {partition_by}
                    {settings_expression}
                    """
                )

    elif storage_type == "azure":
        if run_on_cluster:
            assert table_function
            return f"""
                icebergAzureCluster('cluster_simple', azure, container = '{cluster.azure_container_name}', storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/', format={format})
            """
        else:
            if table_function:
                return f"""
                    icebergAzure(azure, container = '{cluster.azure_container_name}', storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/', format={format})
                """
            else:
                return (
                    f"""
                    DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {if_not_exists_prefix} {table_name} {schema}
                    ENGINE=IcebergAzure(azure, container = {cluster.azure_container_name}, storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/', format={format})
                    {partition_by}
                    {settings_expression}
                    """
                )

    elif storage_type == "local":
        assert not run_on_cluster

        if table_function:
            return f"""
                icebergLocal(local, path = '/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}', format={format})
            """
        else:
            return (
                f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {if_not_exists_prefix} {table_name} {schema}
                ENGINE=IcebergLocal(local, path = '/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}', format={format})
                {partition_by}
                {settings_expression}
                """
            )

    else:
        raise Exception(f"Unknown iceberg storage type: {storage_type}")


def get_raw_schema_and_data(instance, table_expression, timestamp_ms=None):
    if timestamp_ms:
        schema = instance.query(f"DESC {table_expression} SETTINGS iceberg_timestamp_ms = {timestamp_ms}")
        data = instance.query(f"SELECT * FROM {table_expression} ORDER BY ALL SETTINGS iceberg_timestamp_ms = {timestamp_ms}")
    else:
        schema = instance.query(f"DESC {table_expression}")
        data = instance.query(f"SELECT * FROM {table_expression} ORDER BY ALL")
    return schema, data


clickhouse_to_pandas_types = {
    "Int32": "int32",
}

def convert_schema_and_data_to_pandas_df(schema_raw, data_raw):
    # Extract column names from schema
    schema_rows = list(
        map(
            lambda x: x.split("\t")[:2],
            filter(lambda x: len(x) > 0, schema_raw.strip().split("\n")),
        )
    )
    column_names = [x[0] for x in schema_rows]
    types = [x[1] for x in schema_rows]
    pandas_types = [clickhouse_to_pandas_types[t]for t in types]

    schema_df = pd.DataFrame([types], columns=column_names)

    # Convert data to DataFrame
    data_rows = list(
        map(
            lambda x: x.split("\t"),
            filter(lambda x: len(x) > 0, data_raw.strip().split("\n")),
        )
    )

    if data_rows:
        data_df = pd.DataFrame(data_rows, columns=column_names, dtype='object')
    else:
        # Create empty DataFrame with correct columns
        data_df = pd.DataFrame(columns=column_names, dtype='object')

    data_df = data_df.astype(dict(zip(column_names, pandas_types)))
    return schema_df, data_df

def check_schema_and_data(instance, table_expression, expected_schema, expected_data, timestamp_ms=None):
    raw_schema, raw_data = get_raw_schema_and_data(instance, table_expression, timestamp_ms)

    schema = list(
        map(
            lambda x: x.split("\t")[:2],
            filter(lambda x: len(x) > 0, raw_schema.strip().split("\n")),
        )
    )
    data = list(
        map(
            lambda x: x.split("\t"),
            filter(lambda x: len(x) > 0, raw_data.strip().split("\n")),
        )
    )
    assert expected_schema == schema
    assert expected_data == data

def get_uuid_str():
    return str(uuid.uuid4()).replace("-", "_")


def create_iceberg_table(
    storage_type,
    node,
    table_name,
    cluster,
    schema="",
    format_version=2,
    partition_by="",
    if_not_exists=False,
    compression_method=None,
    run_on_cluster=False,
    format="Parquet",
    **kwargs,
):
    if 'output_format_parquet_use_custom_encoder' in kwargs:
        node.query(
            get_creation_expression(storage_type, table_name, cluster, schema, format_version, partition_by, if_not_exists, compression_method, format, run_on_cluster = run_on_cluster, **kwargs),
            settings={"output_format_parquet_use_custom_encoder" : 0, "output_format_parquet_parallel_encoding" : 0}
        )
    else:
        node.query(
            get_creation_expression(storage_type, table_name, cluster, schema, format_version, partition_by, if_not_exists, compression_method, format, run_on_cluster=run_on_cluster, **kwargs),
        )


def drop_iceberg_table(
    node,
    table_name,
    if_exists=False,
):
    if if_exists:
        node.query(f"DROP TABLE IF EXISTS {table_name};")
    else:
        node.query(f"DROP TABLE {table_name};")


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


def default_upload_directory(
    started_cluster, storage_type, local_path, remote_path, **kwargs
):
    prefix = "/var/lib/clickhouse/user_files"
    if local_path != "" and local_path[:len(prefix)] != prefix:
        local_path = prefix + local_path
    if remote_path != "" and remote_path[:len(prefix)] != prefix:
        remote_path = prefix + remote_path

    if storage_type == "local":
        return started_cluster.default_local_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    elif storage_type == "s3":
        print(kwargs)
        return started_cluster.default_s3_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    elif storage_type == "azure":
        return started_cluster.default_azure_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    else:
        raise Exception(f"Unknown iceberg storage type: {storage_type}")


def default_download_directory(
    started_cluster, storage_type, remote_path, local_path, **kwargs
):
    if storage_type == "local":
        return started_cluster.default_local_downloader.download_directory(
            local_path, remote_path, **kwargs
        )
    elif storage_type == "s3":
        return started_cluster.default_s3_downloader.download_directory(
            local_path, remote_path, **kwargs
        )
    else:
        raise Exception(f"Unknown iceberg storage type for downloading: {storage_type}")


def execute_spark_query_general(
    spark, started_cluster, storage_type: str, table_name: str, query: str
):
    spark.sql(query)
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{table_name}/",
        f"/iceberg_data/default/{table_name}/",
    )
    return

def get_last_snapshot(path_to_table):
    import json
    import os

    metadata_dir = f"{path_to_table}/metadata/"
    last_timestamp = 0
    last_snapshot_id = -1
    for filename in os.listdir(metadata_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(metadata_dir, filename)
            with open(filepath, 'r') as f:
                data = json.load(f)
                print(data)
                timestamp = data.get('last-updated-ms')
                if (timestamp > last_timestamp):
                    last_timestamp = timestamp
                    last_snapshot_id = data.get('current-snapshot-id')
    return last_snapshot_id


def check_validity_and_get_prunned_files_general(instance, table_name, settings1, settings2, profile_event_name, select_expression):
    query_id1 = f"{table_name}-{uuid.uuid4()}"
    query_id2 = f"{table_name}-{uuid.uuid4()}"

    data1 = instance.query(
        select_expression,
        query_id=query_id1,
        settings=settings1
    )
    data1 = list(
        map(
            lambda x: x.split("\t"),
            filter(lambda x: len(x) > 0, data1.strip().split("\n")),
        )
    )

    data2 = instance.query(
        select_expression,
        query_id=query_id2,
        settings=settings2
    )
    data2 = list(
        map(
            lambda x: x.split("\t"),
            filter(lambda x: len(x) > 0, data2.strip().split("\n")),
        )
    )

    assert data1 == data2

    instance.query("SYSTEM FLUSH LOGS")

    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['{profile_event_name}'] FROM system.query_log WHERE query_id = '{query_id1}' AND type = 'QueryFinish'"
        )
    )
    return int(
        instance.query(
            f"SELECT ProfileEvents['{profile_event_name}'] FROM system.query_log WHERE query_id = '{query_id2}' AND type = 'QueryFinish'"
        )
    )
