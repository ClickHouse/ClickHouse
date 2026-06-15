import logging
import time

import pytest

logging.getLogger("py4j").setLevel(logging.ERROR)

import pyspark

from helpers.cluster import ClickHouseCluster
from helpers.iceberg_engine import make_engine
from helpers.s3_tools import (
    AzureUploader,
    LocalDownloader,
    LocalUploader,
    S3Downloader,
    S3Uploader,
    prepare_s3_bucket,
)
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config


def get_spark(log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder.appName("test_storage_iceberg_combined")
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
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.sedona.spark.SedonaSparkSessionExtension",
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

_MAIN_CONFIGS = [
    "configs/config.d/query_log.xml",
    "configs/config.d/cluster.xml",
    "configs/config.d/named_collections.xml",
    "configs/config.d/filesystem_caches.xml",
    "configs/config.d/metadata_log.xml",
]
_USER_CONFIGS_SPARK = ["configs/users.d/users_spark.xml"]
_USER_CONFIGS_TRINO = ["configs/users.d/users.xml"]


def _build_spark_cluster():
    cluster = ClickHouseCluster(__file__, name="iceberg_combined_spark", with_spark=True)
    cluster.add_instance(
        "node1",
        main_configs=_MAIN_CONFIGS,
        user_configs=_USER_CONFIGS_SPARK,
        with_minio=True,
        with_azurite=True,
        stay_alive=True,
    )
    for extra in ("node2", "node3"):
        cluster.add_instance(
            extra,
            main_configs=_MAIN_CONFIGS,
            user_configs=_USER_CONFIGS_SPARK,
            stay_alive=True,
        )
    cluster.start()

    prepare_s3_bucket(cluster)
    cluster.spark_session = ResilientSparkSession(
        lambda: get_spark(cluster.instances_dir)
    )
    cluster.default_s3_uploader = S3Uploader(cluster.minio_client, cluster.minio_bucket)
    cluster.azure_container_name = "mycontainer"
    cluster.blob_service_client.create_container(cluster.azure_container_name)
    cluster.default_azure_uploader = AzureUploader(
        cluster.blob_service_client, cluster.azure_container_name
    )
    cluster.default_local_uploader = LocalUploader(cluster.instances["node1"])
    cluster.default_local_downloader = LocalDownloader(cluster.instances["node1"])
    cluster.default_s3_downloader = S3Downloader(cluster.minio_client, cluster.minio_bucket)
    return cluster


def _wait_for_trino_ready(engine, timeout_seconds=120):
    deadline = time.time() + timeout_seconds
    last_err = None
    while time.time() < deadline:
        try:
            out = engine._exec(
                "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'"
            )
            if out.strip().isdigit() and int(out.strip()) >= 1:
                return
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(2)
    raise RuntimeError(f"Trino did not become ready in {timeout_seconds}s: {last_err}")


def _build_trino_cluster():
    cluster = ClickHouseCluster(__file__, name="iceberg_combined_trino")
    cluster.add_instance(
        "node1",
        main_configs=[
            "configs/config.d/cluster.xml",
            "configs/config.d/named_collections.xml",
            "configs/config.d/query_log.xml",
        ],
        user_configs=_USER_CONFIGS_TRINO,
        stay_alive=True,
        with_iceberg_catalog=True,
        extra_parameters={
            "docker_compose_file_name": "docker_compose_iceberg_rest_catalog_with_trino.yml",
        },
    )
    cluster.start()
    return cluster


@pytest.fixture(scope="module", params=["spark", "trino"])
def engine_name(request):
    return request.param


@pytest.fixture(scope="module")
def started_cluster(engine_name):
    if engine_name == "spark":
        cluster = _build_spark_cluster()
    else:
        cluster = _build_trino_cluster()
    try:
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def node(started_cluster):
    return started_cluster.instances["node1"]


@pytest.fixture(scope="module")
def engine(started_cluster, engine_name):
    eng = make_engine(engine_name, started_cluster)
    if engine_name == "trino":
        _wait_for_trino_ready(eng)
    try:
        yield eng
    finally:
        eng.cleanup()


@pytest.fixture
def spark_only(engine):
    if engine.name != "spark":
        pytest.skip("Spark/IcebergS3-specific test; not applicable to the Trino/DataLakeCatalog path")
    return engine
