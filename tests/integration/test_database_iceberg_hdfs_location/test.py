"""A `DataLakeCatalog` table whose catalog location is on HDFS can be read.

The catalog reports the location as `hdfs://...`, and with the default `vended_credentials = 1`
it vends no credentials for it: HDFS authenticates through the server's `hdfs` configuration.
Neither may stop the table from resolving. Reading it still needs `READ ON HDFS` for its location,
as reading it through `icebergHDFS` does. Writing it would use the server's HDFS identity, so it is read-only.
"""

import json
import os
from urllib.parse import urlparse

import pytest
import requests

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.mock_servers import start_mock_servers

if is_arm():
    pytestmark = pytest.mark.skip

CATALOG_PORT = 8182

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_grants.xml", "configs/remote_servers.xml"],
    with_hdfs=True,
)

USER = "hdfs_catalog_reader"
LOCATION = "hdfs://hdfs1:9000/warehouse/ns/events"


def list_hdfs_dir(hdfs_api, path):
    response = requests.get(
        f"{hdfs_api.protocol}://{hdfs_api.hdfs_ip}:{hdfs_api.proxy_port}/webhdfs/v1{path}",
        params={"op": "LISTSTATUS", "user.name": hdfs_api.user},
        headers={"host": str(hdfs_api.hdfs_ip)},
        timeout=hdfs_api.timeout,
    )
    response.raise_for_status()
    return sorted(
        status["pathSuffix"] for status in response.json()["FileStatuses"]["FileStatus"]
    )


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


def test_hdfs_location_requires_hdfs_source_grant(started_cluster, hdfs_catalog):
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

        node.query(f"GRANT READ ON HDFS('{LOCATION}') TO {USER}")
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


def test_hdfs_location_is_read_only(started_cluster, hdfs_catalog):
    # Even the default user, who has every grant, cannot write: the object storage rejects it.
    table_path = urlparse(LOCATION).path
    files_before = list_hdfs_dir(started_cluster.hdfs_api, table_path)
    for settings in [
        "allow_insert_into_iceberg = 1",
        "allow_insert_into_iceberg = 1, async_insert = 1, wait_for_async_insert = 1",
        "allow_insert_into_iceberg = 1, enable_parallel_replicas = 2, max_parallel_replicas = 2, "
        "cluster_for_parallel_replicas = 'one_shard'",
    ]:
        error = node.query_and_get_error(
            f"INSERT INTO hdfs_catalog.`ns.events` SETTINGS {settings} VALUES (1, 'a')"
        )
        assert "READONLY" in error, error
    assert list_hdfs_dir(started_cluster.hdfs_api, table_path) == files_before
