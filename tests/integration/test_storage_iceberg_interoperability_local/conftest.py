import pytest
import logging
import pyspark
import os

from helpers.cluster import ClickHouseCluster
from helpers.iceberg_utils import get_uuid_str
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config


# Each node gets its own iceberg data directory so they don't see each other's writes.
# external_dirs mounts <cluster.instances_dir>/<path> (host) → <path> (container).
# We then create symlinks on the host so that the container path also resolves
# on the host — this way Iceberg metadata with absolute paths works for both
# Spark (host) and ClickHouse (container).
ICEBERG_DIR_NODE1 = "/var/lib/clickhouse/user_files/iceberg_node1"
ICEBERG_DIR_NODE2 = "/var/lib/clickhouse/user_files/iceberg_node2"


def create_host_symlink(container_path, host_path):
    """
    Create a symlink on the host from container_path → host_path.
    After this, both Spark (on host) and ClickHouse (in container)
    can use the same absolute path to access the data.
    """
    os.makedirs(os.path.dirname(container_path), exist_ok=True)
    if os.path.exists(container_path):
        if os.path.islink(container_path):
            os.remove(container_path)
        else:
            return  # Real directory exists (e.g., actual ClickHouse installation), don't touch it
    os.symlink(host_path, container_path)
    logging.info(f"Created symlink: {container_path} → {host_path}")


def cleanup_host_symlink(container_path):
    """Remove symlink created by create_host_symlink."""
    if os.path.islink(container_path):
        os.remove(container_path)
        logging.info(f"Removed symlink: {container_path}")


def get_spark(warehouse_node1, warehouse_node2, log_dir=None):
    builder = (
        pyspark.sql.SparkSession.builder
            .appName("IcebergLocalTwoNodes")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # Catalog for node1
            .config("spark.sql.catalog.node1_catalog",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.node1_catalog.type", "hadoop")
            .config("spark.sql.catalog.node1_catalog.warehouse", warehouse_node1)
            # Catalog for node2
            .config("spark.sql.catalog.node2_catalog",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.node2_catalog.type", "hadoop")
            .config("spark.sql.catalog.node2_catalog.warehouse", warehouse_node2)
            .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.getOrCreate()


@pytest.fixture(scope="package")
def started_cluster_iceberg():
    symlinks = []
    try:
        cluster = ClickHouseCluster(__file__, with_spark=True)
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            mem_limit="15g",
            external_dirs=[ICEBERG_DIR_NODE1],
        )
        cluster.add_instance(
            "node2",
            main_configs=[
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            mem_limit="15g",
            external_dirs=[ICEBERG_DIR_NODE2],
        )

        logging.info("Starting cluster...")
        cluster.start()

        # external_dirs creates:
        #   host: <instances_dir>/var/lib/clickhouse/user_files/iceberg_node1
        #   container: /var/lib/clickhouse/user_files/iceberg_node1
        #
        # Create symlinks on the host so the container path resolves to the
        # host path. Now both Spark and ClickHouse use the same absolute paths.
        for iceberg_dir in [ICEBERG_DIR_NODE1, ICEBERG_DIR_NODE2]:
            host_path = os.path.join(
                cluster.instances_dir, iceberg_dir.lstrip("/")
            )
            create_host_symlink(iceberg_dir, host_path)
            symlinks.append(iceberg_dir)

        # Both Spark and ClickHouse use the container paths.
        # On the host, these resolve via symlinks to the actual data.
        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(ICEBERG_DIR_NODE1, ICEBERG_DIR_NODE2, cluster.instances_dir)
        )

        yield cluster

    finally:
        for link in symlinks:
            cleanup_host_symlink(link)
        cluster.shutdown()


def test_spark_write_and_read(started_cluster_iceberg):
    """Verify Spark can write to and read from local filesystem via Iceberg."""
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_roundtrip_" + get_uuid_str()

    # Write
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    spark.sql(
        f"""
            INSERT INTO node1_catalog.default.{TABLE_NAME}
            SELECT id as number FROM range(100)
        """
    )

    # Read back
    df = spark.sql(
        f"SELECT count(*) as cnt FROM node1_catalog.default.{TABLE_NAME}"
    ).collect()
    count = df[0].cnt
    logging.info(f"Spark read back {count} rows")
    assert count == 100, f"Expected 100 rows, got {count}"
