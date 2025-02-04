import glob
import json
import logging
import os
import time
import uuid
from datetime import datetime

import pyspark
import pytest
from azure.storage.blob import BlobServiceClient
from minio.deleteobjects import DeleteObject
from pyspark.sql.functions import (
    current_timestamp,
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.readwriter import DataFrameWriter, DataFrameWriterV2
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
from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.s3_tools import (
    AzureUploader,
    HDFSUploader,
    LocalUploader,
    S3Uploader,
    get_file_contents,
    list_s3_objects,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

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
        .config("spark.sql.catalog.spark_catalog.warehouse", "/iceberg_data")
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
        with_hdfs = True
        if is_arm():
            with_hdfs = False
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
            with_hdfs=with_hdfs,
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

        cluster.default_hdfs_uploader = HDFSUploader(cluster)

        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])

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
    format="Parquet",
    table_function=False,
    allow_dynamic_metadata_for_data_lakes=False,
    run_on_cluster=False,
    **kwargs,
):
    allow_dynamic_metadata_for_datalakes_suffix = (
        " SETTINGS allow_dynamic_metadata_for_data_lakes = 1"
        if allow_dynamic_metadata_for_data_lakes
        else ""
    )

    if storage_type == "s3":
        if "bucket" in kwargs:
            bucket = kwargs["bucket"]
        else:
            bucket = cluster.minio_bucket

        if run_on_cluster:
            assert table_function
            return f"icebergS3Cluster('cluster_simple', s3, filename = 'iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"
        else:
            if table_function:
                return f"icebergS3(s3, filename = 'iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"
            else:
                return (
                    f"""
                    DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {table_name}
                    ENGINE=IcebergS3(s3, filename = 'iceberg_data/default/{table_name}/', format={format}, url = 'http://minio1:9001/{bucket}/')"""
                    + allow_dynamic_metadata_for_datalakes_suffix
                )

    elif storage_type == "azure":
        if run_on_cluster:
            assert table_function
            return f"""
                icebergAzureCluster('cluster_simple', azure, container = '{cluster.azure_container_name}', storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/iceberg_data/default/{table_name}/', format={format})
            """
        else:
            if table_function:
                return f"""
                    icebergAzure(azure, container = '{cluster.azure_container_name}', storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/iceberg_data/default/{table_name}/', format={format})
                """
            else:
                return (
                    f"""
                    DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {table_name}
                    ENGINE=IcebergAzure(azure, container = {cluster.azure_container_name}, storage_account_url = '{cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]}', blob_path = '/iceberg_data/default/{table_name}/', format={format})"""
                    + allow_dynamic_metadata_for_datalakes_suffix
                )

    elif storage_type == "hdfs":
        if run_on_cluster:
            assert table_function
            return f"""
                icebergHDFSCluster('cluster_simple', hdfs, filename= 'iceberg_data/default/{table_name}/', format={format}, url = 'hdfs://hdfs1:9000/')
            """
        else:
            if table_function:
                return f"""
                    icebergHDFS(hdfs, filename= 'iceberg_data/default/{table_name}/', format={format}, url = 'hdfs://hdfs1:9000/')
                """
            else:
                return (
                    f"""
                    DROP TABLE IF EXISTS {table_name};
                    CREATE TABLE {table_name}
                    ENGINE=IcebergHDFS(hdfs, filename = 'iceberg_data/default/{table_name}/', format={format}, url = 'hdfs://hdfs1:9000/')"""
                    + allow_dynamic_metadata_for_datalakes_suffix
                )

    elif storage_type == "local":
        assert not run_on_cluster

        if table_function:
            return f"""
                icebergLocal(local, path = '/iceberg_data/default/{table_name}/', format={format})
            """
        else:
            return (
                f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name}
                ENGINE=IcebergLocal(local, path = '/iceberg_data/default/{table_name}/', format={format})"""
                + allow_dynamic_metadata_for_datalakes_suffix
            )

    else:
        raise Exception(f"Unknown iceberg storage type: {storage_type}")


def check_schema_and_data(instance, table_expression, expected_schema, expected_data):
    schema = instance.query(f"DESC {table_expression}")
    data = instance.query(f"SELECT * FROM {table_expression} ORDER BY ALL")
    schema = list(
        map(
            lambda x: x.split("\t")[:2],
            filter(lambda x: len(x) > 0, schema.strip().split("\n")),
        )
    )
    data = list(
        map(
            lambda x: x.split("\t"),
            filter(lambda x: len(x) > 0, data.strip().split("\n")),
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
    format="Parquet",
    **kwargs,
):
    node.query(
        get_creation_expression(storage_type, table_name, cluster, format, **kwargs)
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


def default_upload_directory(
    started_cluster, storage_type, local_path, remote_path, **kwargs
):
    if storage_type == "local":
        return started_cluster.default_local_uploader.upload_directory(
            local_path, remote_path, **kwargs
        )
    elif storage_type == "hdfs":
        return started_cluster.default_hdfs_uploader.upload_directory(
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


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_single_iceberg_file(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_single_iceberg_file_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_partition_by(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_partition_by_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 10),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
        partition_by="a",
    )

    files = default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert len(files) == 14  # 10 partitiions + 4 metadata files

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_multiple_iceberg_files(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_multiple_iceberg_files_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    files = default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    # ['/iceberg_data/default/test_multiple_iceberg_files/data/00000-1-35302d56-f1ed-494e-a85b-fbf85c05ab39-00001.parquet',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/version-hint.text',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/3127466b-299d-48ca-a367-6b9b1df1e78c-m0.avro',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/snap-5220855582621066285-1-3127466b-299d-48ca-a367-6b9b1df1e78c.avro',
    # '/iceberg_data/default/test_multiple_iceberg_files/metadata/v1.metadata.json']
    assert len(files) == 5

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="append",
        format_version=format_version,
    )
    files = default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )
    assert len(files) == 9

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 200
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY 1") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(200)"
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_types(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_types_" + format_version + "_" + storage_type + "_" + get_uuid_str()
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
    write_iceberg_from_df(
        spark, df, TABLE_NAME, mode="overwrite", format_version=format_version
    )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 1
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {TABLE_NAME}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster, table_function=True
    )
    assert (
        instance.query(f"SELECT a, b, c, d, e FROM {table_function_expr}").strip()
        == "123\tstring\t2000-01-01\t['str1','str2']\ttrue"
    )

    assert instance.query(f"DESCRIBE {table_function_expr} FORMAT TSV") == TSV(
        [
            ["a", "Nullable(Int32)"],
            ["b", "Nullable(String)"],
            ["c", "Nullable(Date)"],
            ["d", "Array(Nullable(String))"],
            ["e", "Nullable(Bool)"],
        ]
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs"])
def test_cluster_table_function(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")

    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = (
        "test_iceberg_cluster_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def add_df(mode):
        write_iceberg_from_df(
            spark,
            generate_data(spark, 0, 100),
            TABLE_NAME,
            mode=mode,
            format_version=format_version,
        )

        files = default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

        logging.info(f"Adding another dataframe. result files: {files}")

        return files

    files = add_df(mode="overwrite")
    for i in range(1, len(started_cluster.instances)):
        files = add_df(mode="append")

    logging.info(f"Setup complete. files: {files}")
    assert len(files) == 5 + 4 * (len(started_cluster.instances) - 1)

    clusters = instance.query(f"SELECT * FROM system.clusters")
    logging.info(f"Clusters setup: {clusters}")

    # Regular Query only node1
    table_function_expr = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster, table_function=True
    )
    select_regular = (
        instance.query(f"SELECT * FROM {table_function_expr}").strip().split()
    )

    # Cluster Query with node1 as coordinator
    table_function_expr_cluster = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=True,
        run_on_cluster=True,
    )
    select_cluster = (
        instance.query(f"SELECT * FROM {table_function_expr_cluster}").strip().split()
    )

    # Simple size check
    assert len(select_regular) == 600
    assert len(select_cluster) == 600

    # Actual check
    assert select_cluster == select_regular

    # Check query_log
    for replica in started_cluster.instances.values():
        replica.query("SYSTEM FLUSH LOGS")

    for node_name, replica in started_cluster.instances.items():
        cluster_secondary_queries = (
            replica.query(
                f"""
                SELECT query, type, is_initial_query, read_rows, read_bytes FROM system.query_log
                WHERE
                    type = 'QueryStart' AND
                    positionCaseInsensitive(query, '{storage_type}Cluster') != 0 AND
                    position(query, '{TABLE_NAME}') != 0 AND
                    position(query, 'system.query_log') = 0 AND
                    NOT is_initial_query
            """
            )
            .strip()
            .split("\n")
        )

        logging.info(
            f"[{node_name}] cluster_secondary_queries: {cluster_secondary_queries}"
        )
        assert len(cluster_secondary_queries) == 1


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_delete_files(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_delete_files_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
    )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE a >= 0")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 0
    assert instance.contains_in_log("Processing delete file for path")

    write_iceberg_from_df(
        spark,
        generate_data(spark, 100, 200),
        TABLE_NAME,
        mode="upsert",
        format_version=format_version,
    )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE a >= 150")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 50


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
@pytest.mark.parametrize("is_table_function", [False, True])
def test_evolved_schema_simple(
    started_cluster, format_version, storage_type, is_table_function
):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_evolved_schema_simple_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                a int NOT NULL, 
                b float, 
                c decimal(9,2) NOT NULL,
                d array<int>
            )
            USING iceberg 
            OPTIONS ('format-version'='{format_version}')
        """
    )

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster,
        table_function=is_table_function,
        allow_dynamic_metadata_for_data_lakes=True,
    )

    table_select_expression = (
        TABLE_NAME if not is_table_function else table_creation_expression
    )

    if not is_table_function:
        instance.query(table_creation_expression)

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (4, NULL, 7.12, ARRAY(5, 6, 7));
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b TYPE double;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (7, 5.0, 18.1, ARRAY(6, 7, 9));
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "\\N", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN d FIRST;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["a", "Int32"],
            ["b", "Nullable(Float64)"],
            ["c", "Decimal(9, 2)"],
        ],
        [["[5,6,7]", "4", "\\N", "7.12"], ["[6,7,9]", "7", "5", "18.1"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b AFTER d;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(9, 2)"],
        ],
        [["[5,6,7]", "\\N", "4", "7.12"], ["[6,7,9]", "5", "7", "18.1"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMNS (
                e string
            );
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(9, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c TYPE decimal(12, 2);
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(5, 6, 7), 3, -30, 7.12, 'AAA');
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int32"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a TYPE BIGINT;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int64"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(), 3.0, 12, -9.13, 'BBB');
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Int64"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a DROP NOT NULL;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (NULL, 3.4, NULL, -9.13, NULL);
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["d", "Array(Nullable(Int32))"],
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["[]", "3", "12", "-9.13", "BBB"],
            ["[]", "3.4", "\\N", "-9.13", "\\N"],
            ["[5,6,7]", "3", "-30", "7.12", "AAA"],
            ["[5,6,7]", "\\N", "4", "7.12", "\\N"],
            ["[6,7,9]", "5", "7", "18.1", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN d;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["b", "Nullable(Float64)"],
            ["a", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["3", "-30", "7.12", "AAA"],
            ["3", "12", "-9.13", "BBB"],
            ["3.4", "\\N", "-9.13", "\\N"],
            ["5", "7", "18.1", "\\N"],
            ["\\N", "4", "7.12", "\\N"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} RENAME COLUMN a TO f;
        """
    )

    check_schema_and_data(
        instance,
        table_select_expression,
        [
            ["b", "Nullable(Float64)"],
            ["f", "Nullable(Int64)"],
            ["c", "Decimal(12, 2)"],
            ["e", "Nullable(String)"],
        ],
        [
            ["3", "-30", "7.12", "AAA"],
            ["3", "12", "-9.13", "BBB"],
            ["3.4", "\\N", "-9.13", "\\N"],
            ["5", "7", "18.1", "\\N"],
            ["\\N", "4", "7.12", "\\N"],
        ],
    )


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_not_evolved_schema(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_evolved_schema_simple_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                a int NOT NULL, 
                b float, 
                c decimal(9,2) NOT NULL,
                d array<int>
            )
            USING iceberg 
            OPTIONS ('format-version'='{format_version}')
        """
    )

    instance.query(
        get_creation_expression(
            storage_type,
            TABLE_NAME,
            started_cluster,
            table_function=False,
            allow_dynamic_metadata_for_data_lakes=False,
        )
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (4, 3.0, 7.12, ARRAY(5, 6, 7));
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b TYPE double;
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"]],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (7, 5.0, 18.1, ARRAY(6, 7, 9));
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN d FIRST;
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN b AFTER d;
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMNS (
                e string
            );
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN c TYPE decimal(12, 2);
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [["4", "3", "7.12", "[5,6,7]"], ["7", "5", "18.1", "[6,7,9]"]],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(5, 6, 7), 3, -30, 7.12, 'AAA');
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [
            ["-30", "3", "7.12", "[5,6,7]"],
            ["4", "3", "7.12", "[5,6,7]"],
            ["7", "5", "18.1", "[6,7,9]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a TYPE BIGINT;
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [
            ["-30", "3", "7.12", "[5,6,7]"],
            ["4", "3", "7.12", "[5,6,7]"],
            ["7", "5", "18.1", "[6,7,9]"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (ARRAY(), 3.0, 12, -9.13, 'BBB');
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [
            ["-30", "3", "7.12", "[5,6,7]"],
            ["4", "3", "7.12", "[5,6,7]"],
            ["7", "5", "18.1", "[6,7,9]"],
            ["12", "3", "-9.13", "[]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN a DROP NOT NULL;
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [
            ["-30", "3", "7.12", "[5,6,7]"],
            ["4", "3", "7.12", "[5,6,7]"],
            ["7", "5", "18.1", "[6,7,9]"],
            ["12", "3", "-9.13", "[]"],
        ],
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (NULL, 3.4, NULL, -9.13, NULL);
        """
    )

    check_schema_and_data(
        instance,
        TABLE_NAME,
        [
            ["a", "Int32"],
            ["b", "Nullable(Float32)"],
            ["c", "Decimal(9, 2)"],
            ["d", "Array(Nullable(Int32))"],
        ],
        [
            ["-30", "3", "7.12", "[5,6,7]"],
            ["0", "3.4", "-9.13", "[]"],
            ["4", "3", "7.12", "[5,6,7]"],
            ["7", "5", "18.1", "[6,7,9]"],
            ["12", "3", "-9.13", "[]"],
        ],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN d;
        """
    )

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL")

    assert "Not found column" in error


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_evolved_schema_complex(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_evolved_schema_complex_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    def execute_spark_query(query: str):
        spark.sql(query)
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )
        return

    execute_spark_query(
        f"""
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME}   (
                address STRUCT<
                    house_number : DOUBLE,
                    city: STRING,
                    zip: INT
                >,
                animals ARRAY<INT>
            )
            USING iceberg
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES (named_struct('house_number', 3, 'city', 'Singapore', 'zip', 12345), ARRAY(4, 7));
        """
    )

    table_function = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster, table_function=True
    )
    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ADD COLUMNS ( address.appartment INT );
        """
    )

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY ALL")

    assert "UNSUPPORTED_METHOD" in error

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} DROP COLUMN address.appartment;
        """
    )

    check_schema_and_data(
        instance,
        table_function,
        [
            [
                "address",
                "Tuple(\\n    house_number Nullable(Float64),\\n    city Nullable(String),\\n    zip Nullable(Int32))",
            ],
            ["animals", "Array(Nullable(Int32))"],
        ],
        [["(3,'Singapore',12345)", "[4,7]"]],
    )

    execute_spark_query(
        f"""
            ALTER TABLE {TABLE_NAME} ALTER COLUMN animals.element TYPE BIGINT
        """
    )

    error = instance.query_and_get_error(f"SELECT * FROM {table_function} ORDER BY ALL")

    assert "UNSUPPORTED_METHOD" in error


@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_row_based_deletes(started_cluster, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_row_based_deletes_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )
    spark.sql(
        f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100)"
    )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 10")
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        "",
    )

    error = instance.query_and_get_error(f"SELECT * FROM {TABLE_NAME}")
    assert "UNSUPPORTED_METHOD" in error


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_schema_inference(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    for format in ["Parquet", "ORC", "Avro"]:
        TABLE_NAME = (
            "test_schema_inference_"
            + format
            + "_"
            + format_version
            + "_"
            + storage_type
            + "_"
            + get_uuid_str()
        )

        # Types time, timestamptz, fixed are not supported in Spark.
        spark.sql(
            f"CREATE TABLE {TABLE_NAME} (intC int, longC long, floatC float, doubleC double, decimalC1 decimal(10, 3), decimalC2 decimal(20, 10), decimalC3 decimal(38, 30), dateC date,  timestampC timestamp, stringC string, binaryC binary, arrayC1 array<int>, mapC1 map<string, string>, structC1 struct<field1: int, field2: string>, complexC array<struct<field1: map<string, array<map<string, int>>>, field2: struct<field3: int, field4: string>>>) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}', 'write.format.default' = '{format}')"
        )

        spark.sql(
            f"insert into {TABLE_NAME} select 42, 4242, 42.42, 4242.4242, decimal(42.42), decimal(42.42), decimal(42.42), date('2020-01-01'), timestamp('2020-01-01 20:00:00'), 'hello', binary('hello'), array(1,2,3), map('key', 'value'), struct(42, 'hello'), array(struct(map('key', array(map('key', 42))), struct(42, 'hello')))"
        )
        default_upload_directory(
            started_cluster,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

        create_iceberg_table(
            storage_type, instance, TABLE_NAME, started_cluster, format=format
        )

        res = instance.query(
            f"DESC {TABLE_NAME} FORMAT TSVRaw", settings={"print_pretty_type_names": 0}
        )
        expected = TSV(
            [
                ["intC", "Nullable(Int32)"],
                ["longC", "Nullable(Int64)"],
                ["floatC", "Nullable(Float32)"],
                ["doubleC", "Nullable(Float64)"],
                ["decimalC1", "Nullable(Decimal(10, 3))"],
                ["decimalC2", "Nullable(Decimal(20, 10))"],
                ["decimalC3", "Nullable(Decimal(38, 30))"],
                ["dateC", "Nullable(Date)"],
                ["timestampC", "Nullable(DateTime64(6, 'UTC'))"],
                ["stringC", "Nullable(String)"],
                ["binaryC", "Nullable(String)"],
                ["arrayC1", "Array(Nullable(Int32))"],
                ["mapC1", "Map(String, Nullable(String))"],
                ["structC1", "Tuple(field1 Nullable(Int32), field2 Nullable(String))"],
                [
                    "complexC",
                    "Array(Tuple(field1 Map(String, Array(Map(String, Nullable(Int32)))), field2 Tuple(field3 Nullable(Int32), field4 Nullable(String))))",
                ],
            ]
        )

        assert res == expected

        # Check that we can parse data
        instance.query(f"SELECT * FROM {TABLE_NAME}")


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_metadata_file_selection(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_metadata_selection_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )

    for i in range(50):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10)"
        )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "hdfs", "local"])
def test_metadata_file_format_with_uuid(started_cluster, format_version, storage_type):
    if is_arm() and storage_type == "hdfs":
        pytest.skip("Disabled test IcebergHDFS for aarch64")
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        "test_metadata_selection_with_uuid_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )

    for i in range(50):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10)"
        )

    for i in range(50):
        os.rename(
            f"/iceberg_data/default/{TABLE_NAME}/metadata/v{i + 1}.metadata.json",
            f"/iceberg_data/default/{TABLE_NAME}/metadata/{str(i).zfill(5)}-{get_uuid_str()}.metadata.json",
        )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500


def test_restart_broken_s3(started_cluster):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_restart_broken_table_function_s3" + "_" + get_uuid_str()

    minio_client = started_cluster.minio_client
    bucket = "broken2"

    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 100),
        TABLE_NAME,
        mode="overwrite",
        format_version="1",
    )

    files = default_upload_directory(
        started_cluster,
        "s3",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
        bucket=bucket,
    )
    create_iceberg_table("s3", instance, TABLE_NAME, started_cluster, bucket=bucket)
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

    files = default_upload_directory(
        started_cluster,
        "s3",
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
        bucket=bucket,
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100


@pytest.mark.parametrize("storage_type", ["s3"])
def test_filesystem_cache(started_cluster, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = "test_filesystem_cache_" + storage_type + "_" + get_uuid_str()

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 10),
        TABLE_NAME,
        mode="overwrite",
        format_version="1",
        partition_by="a",
    )

    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster)

    query_id = f"{TABLE_NAME}-{uuid.uuid4()}"
    instance.query(
        f"SELECT * FROM {TABLE_NAME} SETTINGS filesystem_cache_name = 'cache1'",
        query_id=query_id,
    )

    instance.query("SYSTEM FLUSH LOGS")

    written_to_cache_first_select = int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    read_from_cache_first_select = int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
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

    read_from_cache_second_select = int(
        instance.query(
            f"SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )

    assert (
        read_from_cache_second_select
        == read_from_cache_first_select + written_to_cache_first_select
    )

    assert 0 == int(
        instance.query(
            f"SELECT ProfileEvents['S3GetObject'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
