import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster

CATALOG_URL = "http://seaweedfs:8181/v1"
STORAGE_URL = "http://seaweedfs:8333/analytics"
ACCESS_KEY = "clickhouse"
SECRET_KEY = "clickhouse"


def wait_for_seaweedfs(cluster, timeout=120):
    """Wait until both the Iceberg REST catalog and the pre-created bucket are ready."""
    catalog_url = f"http://localhost:{cluster.iceberg_rest_catalog_port}/v1/config"
    deadline = time.monotonic() + timeout
    while True:
        try:
            if requests.get(catalog_url, timeout=2).status_code == 200:
                buckets = cluster.exec_in_container(
                    cluster.get_container_id("seaweedfs"),
                    ["sh", "-c", "echo s3.bucket.list | weed shell 2>/dev/null"],
                )
                if "analytics" in buckets:
                    return
        except Exception:
            if time.monotonic() > deadline:
                raise
        if time.monotonic() > deadline:
            raise TimeoutError("SeaweedFS did not become ready")
        time.sleep(0.5)


@pytest.fixture(scope="module")
def started_cluster():
    cluster = ClickHouseCluster(__file__)
    try:
        cluster.add_instance(
            "node1",
            with_iceberg_catalog=True,
            extra_parameters={
                "docker_compose_file_name": "docker_compose_iceberg_seaweedfs_catalog.yml"
            },
        )
        cluster.start()
        wait_for_seaweedfs(cluster)
        yield cluster
    finally:
        cluster.shutdown()


def test_create_insert_select(started_cluster):
    node = started_cluster.instances["node1"]

    node.query(
        f"""
        CREATE DATABASE lake
        ENGINE = DataLakeCatalog('{CATALOG_URL}', '{ACCESS_KEY}', '{SECRET_KEY}')
        SETTINGS catalog_type = 'rest',
            warehouse = 's3://analytics',
            storage_endpoint = '{STORAGE_URL}',
            catalog_credential = '{ACCESS_KEY}:{SECRET_KEY}',
            oauth_server_uri = '{CATALOG_URL}/oauth/tokens'
        """,
        settings={"allow_experimental_database_iceberg": 1},
    )

    node.query(
        f"""
        CREATE TABLE lake.`sales.returns` (id Int64, reason String)
        ENGINE = IcebergS3('{STORAGE_URL}/sales/returns/', '{ACCESS_KEY}', '{SECRET_KEY}')
        """,
        settings={
            "allow_experimental_database_iceberg": 1,
            "write_full_path_in_iceberg_metadata": 1,
        },
    )

    node.query(
        "INSERT INTO lake.`sales.returns` VALUES (1, 'damaged'), (2, 'wrong size')",
        settings={"allow_experimental_insert_into_iceberg": 1},
    )

    assert (
        node.query("SELECT * FROM lake.`sales.returns` ORDER BY id")
        == "1\tdamaged\n2\twrong size\n"
    )
