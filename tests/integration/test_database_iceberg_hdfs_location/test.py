"""A `DataLakeCatalog` table whose catalog location is on HDFS resolves to `IcebergHDFS`.

The catalog reports the location as `hdfs://...`, and with the default `vended_credentials = 1`
it vends no credentials for it: HDFS authenticates through the server's `hdfs` configuration.
Neither may stop the table from resolving.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

CATALOG_PORT = 8182

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.dirname(__file__),
            [("rest_catalog_mock.py", "node", CATALOG_PORT)],
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_hdfs_location_with_vended_credentials(started_cluster):
    if (
        node.query(
            "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_HDFS'"
        )
        != "1\n"
    ):
        pytest.skip("ClickHouse is built without HDFS")

    node.query(f"""
        DROP DATABASE IF EXISTS hdfs_catalog;
        SET allow_database_iceberg = 1;
        CREATE DATABASE hdfs_catalog
        ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1')
        SETTINGS catalog_type = 'rest', warehouse = 'warehouse'
        """)
    try:
        create_query = node.query("SHOW CREATE TABLE hdfs_catalog.`ns.events`")
        assert "IcebergHDFS" in create_query, create_query
        assert "hdfs://hdfs1:9000/warehouse/ns/events" in create_query, create_query

        # Builds the table's storage. The storage connects to HDFS lazily, so no HDFS is needed.
        assert (
            node.query(
                "DESCRIBE hdfs_catalog.`ns.events` FORMAT TSVRaw SETTINGS describe_compact_output = 1"
            )
            == "id\tInt64\nname\tNullable(String)\n"
        )
    finally:
        node.query("DROP DATABASE IF EXISTS hdfs_catalog")
