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
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_spark(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_storage_iceberg_disks")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config(
            "spark.sql.catalog.spark_catalog.warehouse",
            "/var/lib/clickhouse/user_files/iceberg_data",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.getOrCreate()


def generate_cluster_def(common_path, port, azure_container):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "./_gen/named_collections.xml",
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(
            f"""
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_local_common>
                <type>local</type>
                <path>/var/lib/clickhouse/user_files/iceberg_data/default/</path>
            </disk_local_common>
            <disk_s3_common>
                <type>s3</type>
                <endpoint>http://minio1:9001/root/var/lib/clickhouse/user_files/iceberg_data/default/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>
            </disk_s3_common>
            <disk_azure_common>
                <type>object_storage</type>
                <object_storage_type>azure_blob_storage</object_storage_type>
                <storage_account_url>http://azurite1:{port}/devstoreaccount1</storage_account_url>
                <container_name>{azure_container}</container_name>
                <skip_access_check>false</skip_access_check>
                <account_name>devstoreaccount1</account_name>
                <account_key>Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==</account_key>
            </disk_azure_common>
        </disks>
    </storage_configuration>
    <allowed_disks_for_table_engines>disk_local_common,disk_s3_common,disk_azure_common</allowed_disks_for_table_engines>
</clickhouse>
"""
        )
    return path


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        port = cluster.azurite_port
        user_files_path = os.path.join(
            SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
        )
        conf_path = generate_cluster_def(user_files_path + "/", port, "mycontainer")

        cluster.add_instance(
            "node1",
            main_configs=[conf_path, "configs/cluster.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=[conf_path, "configs/cluster.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node3",
            main_configs=[conf_path, "configs/cluster.xml"],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()

        prepare_s3_bucket(cluster)
        logging.info("S3 bucket created")

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(cluster.instances_dir)
        )
        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        cluster.azure_container_name = "mycontainer"

        cluster.blob_service_client = cluster.blob_service_client

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
@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_single_iceberg_file(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    TABLE_NAME = f"test_single_iceberg_file_{get_uuid_str()}"

    write_iceberg_from_df(spark, generate_data(spark, 0, 100), TABLE_NAME)
    default_upload_directory(
        started_cluster,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_name_1 = f"{TABLE_NAME}_{storage_type}_1"
    table_name_2 = f"{TABLE_NAME}_{storage_type}_2"
    table_name_3 = f"{TABLE_NAME}_{storage_type}_3"
    table_name_4 = f"{TABLE_NAME}_{storage_type}_4"
    table_name_5 = f"{TABLE_NAME}_{storage_type}_5"

    storage_path = (
        f"{TABLE_NAME}"
        if storage_type != "azure"
        else f"var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}"
    )
    assert "Path suffixes" in instance.query_and_get_error(
        f"CREATE TABLE {table_name_1} ENGINE=Iceberg('../', 'Parquet') SETTINGS disk = 'disk_{storage_type}_common'"
    )
    assert "Path suffixes" in instance.query_and_get_error(
        f"CREATE TABLE {table_name_1} ENGINE=Iceberg('/var/lib/clickhouse/user_files/default', 'Parquet') SETTINGS disk = 'disk_{storage_type}_common'"
    )

    instance.query(
        f"CREATE TABLE {table_name_2} ENGINE=Iceberg('{storage_path}', 'Parquet') SETTINGS disk = 'disk_{storage_type}_common'"
    )
    assert instance.query(f"SELECT * FROM {table_name_2}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(
        f"CREATE TABLE {table_name_3} ENGINE=Iceberg(path = '{storage_path}', format = Parquet) SETTINGS disk = 'disk_{storage_type}_common'"
    )
    assert instance.query(f"SELECT * FROM {table_name_3}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(
        f"CREATE TABLE {table_name_4} ENGINE=Iceberg(path = '{storage_path}', format = Parquet, compression_method = 'auto') SETTINGS disk = 'disk_{storage_type}_common'"
    )
    assert instance.query(f"SELECT * FROM {table_name_4}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    instance.query(
        f"CREATE TABLE {table_name_5} ENGINE=Iceberg(path = '{storage_path}', format = Parquet, compression_method = 'auto') SETTINGS disk = 'disk_{storage_type}_common'"
    )
    assert instance.query(f"SELECT * FROM {table_name_5}") == instance.query(
        "SELECT number, toString(number + 1) FROM numbers(100)"
    )

    assert instance.query(
        f"SELECT * FROM iceberg(path = '{storage_path}', SETTINGS disk = 'disk_{storage_type}_common')"
    ) == instance.query("SELECT number, toString(number + 1) FROM numbers(100)")

    if storage_type == "s3":
        with pytest.raises(Exception):
            instance.query(
                f"SELECT * FROM icebergLocal(path = '{storage_path}', SETTINGS disk = 'disk_{storage_type}_common')"
            )
        instance.query(
            f"SELECT * FROM icebergS3(path = '{storage_path}',  SETTINGS disk = 'disk_{storage_type}_common')"
        )


@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_many_tables(started_cluster, format_version, storage_type):
    instance = started_cluster.instances["node1"]
    TABLE_NAME = f"test_many_tables_{get_uuid_str()}"

    table_name = f"{TABLE_NAME}_{storage_type}"
    table_name_2 = f"{TABLE_NAME}_{storage_type}_2"

    storage_path = (
        f"{table_name}"
        if storage_type != "azure"
        else f"iceberg_data/default/{table_name}"
    )
    storage_path_2 = (
        f"{table_name_2}"
        if storage_type != "azure"
        else f"iceberg_data/default/{table_name_2}"
    )

    instance.query(
        f"CREATE TABLE {table_name} (col INT) ENGINE=Iceberg(path = '{storage_path}', format = Parquet, compression_method = 'auto') SETTINGS disk = 'disk_{storage_type}_common'",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"CREATE TABLE {table_name_2} (col INT) ENGINE=Iceberg(path = '{storage_path_2}', format = Parquet, compression_method = 'auto') SETTINGS disk = 'disk_{storage_type}_common'",
        settings={"allow_insert_into_iceberg": 1},
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )
    instance.query(
        f"INSERT INTO {table_name_2} VALUES (1);",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT * FROM {table_name}") == "1\n"
    assert instance.query(f"SELECT * FROM {table_name_2}") == "1\n"

    instance.query(f"DROP TABLE {table_name_2}")
    instance.query(f"DROP TABLE {table_name}")


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_cluster_table_function(started_cluster, storage_type):

    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session

    TABLE_NAME = f"test_iceberg_cluster_{get_uuid_str()}"

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
    table_function_expr = (
        f"iceberg('{TABLE_NAME}', SETTINGS disk = 'disk_{storage_type}_common')"
        if storage_type == "s3"
        else f"iceberg('var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}', SETTINGS disk = 'disk_{storage_type}_common')"
    )
    select_regular = (
        instance.query(f"SELECT * FROM {table_function_expr}").strip().split()
    )

    # Cluster Query with node1 as coordinator
    table_function_expr_cluster = (
        f"icebergCluster('cluster_simple', '{TABLE_NAME}', SETTINGS disk = 'disk_{storage_type}_common')"
        if storage_type == "s3"
        else f"icebergCluster('cluster_simple', 'var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}', SETTINGS disk = 'disk_{storage_type}_common')"
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

    # write 3 times
    assert (
        int(instance.query(f"SELECT count() FROM {table_function_expr_cluster}"))
        == 100 * 3
    )
