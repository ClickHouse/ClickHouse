import os

import pyarrow as pa
import pytest
from deltalake.writer import write_deltalake

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MOCK_PORT = 8080
REMOTE_STORAGE_ROOT = "/var/lib/clickhouse/user_files/unity_gcs_storage"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


def copy_directory_to_container(local_root, container_id, remote_root):
    cluster.exec_in_container(container_id, ["mkdir", "-p", remote_root])
    for root, _, files in os.walk(local_root):
        relative_root = os.path.relpath(root, local_root)
        remote_directory = remote_root if relative_root == "." else os.path.join(remote_root, relative_root)
        cluster.exec_in_container(container_id, ["mkdir", "-p", remote_directory])
        for filename in files:
            cluster.copy_file_to_container(
                container_id,
                os.path.join(root, filename),
                os.path.join(remote_directory, filename),
            )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        local_table = os.path.join(cluster.instances_dir, "unity_gcs_table")
        write_deltalake(
            local_table,
            pa.table({"value": pa.array([1, 2, 3], type=pa.int64())}),
        )

        resolver_id = cluster.get_container_id("resolver")
        copy_directory_to_container(local_table, resolver_id, os.path.join(REMOTE_STORAGE_ROOT, "table"))
        start_mock_servers(
            cluster,
            SCRIPT_DIR,
            [("unity_gcs_mock.py", "resolver", str(MOCK_PORT), [REMOTE_STORAGE_ROOT])],
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_gcs_vended_credentials_fall_back_from_delta_kernel(started_cluster):
    node.query(
        f"""
        CREATE DATABASE unity
        ENGINE = DataLakeCatalog('http://resolver:{MOCK_PORT}/api/2.1/unity-catalog')
        SETTINGS warehouse = 'warehouse', catalog_credential = 'catalog-token', catalog_type = 'unity',
            vended_credentials = true, storage_endpoint = 'http://resolver:{MOCK_PORT}/storage', storage_uri_style = 'path'
        """,
        settings={"allow_experimental_database_unity_catalog": 1},
    )

    result = node.query(
        "SELECT value FROM unity.`namespace.table` ORDER BY value",
        settings={"allow_delta_kernel_rs": 1},
    )
    assert result == "1\n2\n3\n"
    assert node.contains_in_log(
        "Using the native Delta Lake metadata reader because Delta Kernel does not support Authorization headers"
    )
