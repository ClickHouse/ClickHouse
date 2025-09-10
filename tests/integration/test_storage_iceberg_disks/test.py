import logging
import os

import pyspark
import pytest
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
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
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_amd.xml", "configs/cluster.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/storage_amd.xml", "configs/cluster.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=["configs/storage_amd.xml", "configs/cluster.xml"],
            with_minio=True,
            with_azurite=True,
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

        yield cluster
    finally:
        cluster.shutdown()

def get_uuid_str():
    return str(uuid.uuid4()).replace("-", "_")


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_single_iceberg_file(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = (
        f"test_single_iceberg_file_{get_uuid_str()}"
    )

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_name_2 = f"{TABLE_NAME}_{storage_type}_2"
    table_name_3 = f"{TABLE_NAME}_{storage_type}_3"
    table_name_4 = f"{TABLE_NAME}_{storage_type}_4"
    table_name_5 = f"{TABLE_NAME}_{storage_type}_5"

    instance.query(f"CREATE TABLE {table_name_2} ENGINE=Iceberg('{TABLE_NAME}', 'Parquet') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'")
    assert instance.query(f"SELECT * FROM {table_name_2}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(f"CREATE TABLE {table_name_3} ENGINE=Iceberg(path = '{TABLE_NAME}', format = Parquet) SETTINGS datalake_disk_name = 'disk_{storage_type}_common'")
    assert instance.query(f"SELECT * FROM {table_name_3}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(f"CREATE TABLE {table_name_4} ENGINE=Iceberg(path = '{TABLE_NAME}', format = Parquet, compression_method = 'auto') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'")
    assert instance.query(f"SELECT * FROM {table_name_4}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(f"CREATE TABLE {table_name_5} ENGINE=Iceberg(path = '{TABLE_NAME}', format = Parquet, compression_method = 'auto') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'")
    assert instance.query(f"SELECT * FROM {table_name_5}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    assert instance.query(f"SELECT * FROM iceberg(path = '{TABLE_NAME}') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    if storage_type != "local":
        with pytest.raises(Exception):
            instance.query(f"SELECT * FROM icebergLocal(path = '{TABLE_NAME}') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'")
        instance.query(f"SELECT * FROM icebergS3(path = '{TABLE_NAME}') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'")


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_many_tables(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    TABLE_NAME = (
        f"test_many_tables_{get_uuid_str()}"
    )

    table_name = f"{TABLE_NAME}_{storage_type}"
    table_name_2 = f"{TABLE_NAME}_{storage_type}_2"

    instance.query(f"CREATE TABLE {table_name} (col INT) ENGINE=Iceberg(path = '{table_name}', format = Parquet, compression_method = 'auto') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'", settings={"allow_experimental_insert_into_iceberg": 1})
    instance.query(f"CREATE TABLE {table_name_2} (col INT) ENGINE=Iceberg(path = '{table_name_2}', format = Parquet, compression_method = 'auto') SETTINGS datalake_disk_name = 'disk_{storage_type}_common'", settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"INSERT INTO {table_name} VALUES (1);", settings={"allow_experimental_insert_into_iceberg": 1})
    instance.query(f"INSERT INTO {table_name_2} VALUES (1);", settings={"allow_experimental_insert_into_iceberg": 1})

    assert instance.query(f"SELECT * FROM {table_name}") == "1\n"
    assert instance.query(f"SELECT * FROM {table_name_2}") == "1\n"

    instance.query(f"DROP TABLE {table_name_2}")
    instance.query(f"DROP TABLE {table_name}")

@pytest.mark.parametrize("storage_type", ["s3"])
def test_cluster_table_function(started_cluster, storage_type):

    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = (
        "test_iceberg_cluster"
    )

    def add_df(mode):
        write_iceberg_from_df(
            spark,
            generate_data(spark, 0, 100),
            TABLE_NAME,
            mode=mode,
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
    table_function_expr = f"iceberg('{TABLE_NAME}')"
    select_regular = (
        instance.query(f"SELECT * FROM {table_function_expr} SETTINGS datalake_disk_name = 'disk_s3_common'").strip().split()
    )

    # Cluster Query with node1 as coordinator
    table_function_expr_cluster = f"icebergCluster('cluster_simple', '{TABLE_NAME}')"
    select_cluster = (
        instance.query(f"SELECT * FROM {table_function_expr_cluster} SETTINGS datalake_disk_name = 'disk_s3_common'").strip().split()
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

    # write 3 times
    assert int(instance.query(f"SELECT count() FROM {table_function_expr_cluster} SETTINGS datalake_disk_name = 'disk_s3_common'")) == 100 * 3
