"""A `DataLakeCatalog` table whose catalog location is on HDFS can be read.

The catalog reports the location as `hdfs://...`, and with the default `vended_credentials = 1`
it vends no credentials for it: HDFS authenticates through the server's `hdfs` configuration.
Neither may stop the table from resolving. Reading it still needs `READ ON HDFS` for its location,
as reading it through `icebergHDFS` does.
"""

import json
import os
from urllib.parse import urlparse

import pytest

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.mock_servers import start_mock_servers

if is_arm():
    pytestmark = pytest.mark.skip

CATALOG_PORT = 8182

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/enable_grants.xml"], with_hdfs=True
)

USER = "hdfs_catalog_reader"
LOCATION = "hdfs://hdfs1:9000/warehouse/ns/events"


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


@pytest.fixture(scope="module")
def hdfs_catalog(started_cluster):
    # Write the table metadata the catalog serves to the location it advertises for it.
    # The table has no snapshot, so this one file is the whole table.
    load_table = json.loads(
        node.exec_in_container(
            [
                "curl",
                "-s",
                f"http://localhost:{CATALOG_PORT}/v1/namespaces/ns/tables/events",
            ]
        )
    )
    started_cluster.hdfs_api.write_data(
        urlparse(load_table["metadata-location"]).path,
        json.dumps(load_table["metadata"]),
    )

    node.query(f"""
        DROP DATABASE IF EXISTS hdfs_catalog;
        SET allow_database_iceberg = 1;
        CREATE DATABASE hdfs_catalog
        ENGINE = DataLakeCatalog('http://localhost:{CATALOG_PORT}/v1')
        SETTINGS catalog_type = 'rest', warehouse = 'warehouse'
        """)
    yield
    node.query("DROP DATABASE IF EXISTS hdfs_catalog")


def test_hdfs_location_with_vended_credentials(hdfs_catalog):
    create_query = node.query(
        "SHOW CREATE TABLE hdfs_catalog.`ns.events` FORMAT TSVRaw"
    )
    assert f"ENGINE = Iceberg('{LOCATION}" in create_query, create_query

    assert (
        node.query(
            "DESCRIBE hdfs_catalog.`ns.events` FORMAT TSVRaw SETTINGS describe_compact_output = 1"
        )
        == "id\tInt64\nname\tNullable(String)\n"
    )
    assert node.query("SELECT count() FROM hdfs_catalog.`ns.events`") == "0\n"


def test_hdfs_location_requires_hdfs_source_grant(hdfs_catalog):
    node.query(f"""
        DROP USER IF EXISTS {USER};
        CREATE USER {USER};
        GRANT SELECT, SHOW ON hdfs_catalog.* TO {USER};
        REVOKE SOURCES ON *.* FROM {USER};
        """)

    def assert_denied():
        for query in [
            "DESCRIBE hdfs_catalog.`ns.events`",
            "SELECT count() FROM hdfs_catalog.`ns.events`",
        ]:
            error = node.query_and_get_error(query, user=USER)
            assert "ACCESS_DENIED" in error, error

    try:
        assert_denied()

        node.query(f"GRANT READ ON HDFS('hdfs://hdfs1:9000/elsewhere/.*') TO {USER}")
        assert_denied()

        node.query(f"GRANT READ ON HDFS('hdfs://hdfs1:9000/warehouse/.*') TO {USER}")
        assert (
            node.query(
                "DESCRIBE hdfs_catalog.`ns.events` FORMAT TSVRaw SETTINGS describe_compact_output = 1",
                user=USER,
            )
            == "id\tInt64\nname\tNullable(String)\n"
        )
        assert (
            node.query("SELECT count() FROM hdfs_catalog.`ns.events`", user=USER)
            == "0\n"
        )
    finally:
        node.query(f"DROP USER IF EXISTS {USER}")
