import glob
import json
import logging
import os
import uuid

import pyspark
import pytest
from pyspark.sql.functions import (
    monotonically_increasing_id,
    row_number,
)
from pyspark.sql.types import (
    StringType,
)
from pyspark.sql.window import Window

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    list_s3_objects,
    prepare_s3_bucket,
)
from helpers.test_tools import TSV

from helpers.iceberg_utils import (
    default_upload_directory,
)


SCRIPT_DIR = "/var/lib/clickhouse/user_files" + os.path.join(
    os.path.dirname(os.path.realpath(__file__))
)
cluster = ClickHouseCluster(__file__, with_spark=True)


def get_spark():
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

    return builder.master("local").getOrCreate()


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
                <path>{common_path}</path>
            </disk_local_common>
            <disk_s3_0_common>
                <type>s3</type>
                <endpoint>http://minio1:9001/root/var/lib/clickhouse/user_files/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>
                <no_sign_request>0</no_sign_request>
            </disk_s3_0_common>
            <disk_s3_1_common>
                <type>s3</type>
                <endpoint>http://minio1:9001/root/var/lib/clickhouse/user_files/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>
                <no_sign_request>0</no_sign_request>
            </disk_s3_1_common>
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
    <allowed_disks_for_table_engines>disk_s3_1_common,disk_s3_0_common,disk_local_common,disk_azure_common</allowed_disks_for_table_engines>
</clickhouse>
"""
        )
    return path


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        user_files_path = os.path.join(
            SCRIPT_DIR, f"{cluster.instances_dir_name}/node1/database/user_files"
        )
        port = cluster.azurite_port
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
        cluster.add_instance(
            "node_with_disabled_delta_kernel",
            main_configs=[conf_path, "configs/cluster.xml"],
            user_configs=[
                "configs/users.xml",
                "configs/disabled_delta_kernel.xml",
            ],
            with_minio=True,
            with_azurite=True,
            stay_alive=True,
            with_zookeeper=True,
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

        cluster.default_azure_uploader = AzureUploader(
            cluster.blob_service_client, cluster.azure_container_name
        )

        cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
        cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])

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


def get_uuid_str():
    return str(uuid.uuid4()).replace("-", "_")


def create_delta_table(
    instance,
    storage_type,
    table_name,
    cluster,
    use_delta_kernel,
    path_suffix,
    disk_suffix,
    **kwargs,
):
    if storage_type == "s3":
        instance.query(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
            ENGINE=DeltaLake({path_suffix})
            SETTINGS disk = 'disk_s3_{use_delta_kernel}{disk_suffix}'
            """
        )

    elif storage_type == "azure":
        instance.query(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
            ENGINE=DeltaLake({path_suffix})
            SETTINGS disk = 'disk_azure{disk_suffix}'
            """
        )
    elif storage_type == "local":
        instance.query(
            f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name}
            ENGINE=DeltaLake({path_suffix})
            SETTINGS disk = 'disk_local{disk_suffix}'
            """
        )
    else:
        raise Exception(f"Unknown delta lake storage type: {storage_type}")


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
        os.path.join(os.path.dirname(os.path.realpath(__file__))),
        f"{cluster.instances_dir_name}/{node_name}/database/user_files",
    )
    result_path = f"{user_files_path}/{table_name}.parquet"
    return result_path


@pytest.mark.parametrize(
    "use_delta_kernel, storage_type",
    [("1", "s3"), ("0", "s3"), ("1", "local"), ("0", "azure")],
)
def test_single_log_file(started_cluster, use_delta_kernel, storage_type):
    instance = get_node(started_cluster, use_delta_kernel)
    spark = started_cluster.spark_session
    TABLE_NAME = f"test_single_log_file_{get_uuid_str()}"

    inserted_data = "SELECT number as a, toString(number + 1) as b FROM numbers(100)"
    parquet_data_path = create_initial_data_file(
        started_cluster,
        instance,
        inserted_data,
        TABLE_NAME + "_" + storage_type,
        node_name=instance.name,
    )

    user_files_path = os.path.join(
        SCRIPT_DIR, f"{cluster.instances_dir_name}/{instance.name}/database/user_files"
    )
    table_path = os.path.join(user_files_path, TABLE_NAME)

    # We need to exclude the leading slash for local storage protocol file://
    delta_path = (
        table_path
        if storage_type == "local"
        else f"/var/lib/clickhouse/user_files/{TABLE_NAME}"
    )
    write_delta_from_file(spark, parquet_data_path, delta_path)

    files = default_upload_directory(
        started_cluster,
        storage_type,
        delta_path,
        TABLE_NAME,
    )

    assert len(files) == 2  # 1 metadata files + 1 data file

    create_delta_table(
        instance,
        storage_type,
        TABLE_NAME,
        started_cluster,
        use_delta_kernel,
        (
            f"'{TABLE_NAME}'"
            if storage_type != "azure"
            else f"'var/lib/clickhouse/user_files/{TABLE_NAME}'"
        ),
        "_common",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100
    assert instance.query(f"SELECT * FROM {TABLE_NAME}") == instance.query(
        inserted_data
    )

    if storage_type == "s3":
        disk_name = f"disk_s3_{use_delta_kernel}_common"
    elif storage_type == "local":
        disk_name = f"disk_local_common"
    else:
        disk_name = f"disk_azure_common"

    storage_path = (
        f"{TABLE_NAME}"
        if storage_type != "azure"
        else f"var/lib/clickhouse/user_files/{TABLE_NAME}"
    )
    assert instance.query(
        f"SELECT * FROM deltaLake('{storage_path}', SETTINGS disk = '{disk_name}')"
    ) == instance.query(inserted_data)

    if storage_type == "s3":
        assert instance.query(
            f"SELECT * FROM deltaLakeCluster('cluster_simple', '{storage_path}', SETTINGS disk = '{disk_name}')"
        ) == instance.query(inserted_data)

    instance.query(f"DROP TABLE {TABLE_NAME}")
