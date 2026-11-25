"""Tightening `remote_url_allow_hosts` must not make a server with an `HDFS` database unbootable.

Startup rebuilds every database by replaying its stored `ATTACH DATABASE` statement, and
`loadMetadata` aborts on the first exception, so a host check that throws there takes every other
database down with it. The check belongs to the use of the database, not to its metadata replay.
"""

import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
# No `remote_url_allow_hosts` to begin with: that is the state the stored definition is created in.
node = cluster.add_instance("node", main_configs=[], stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_server_starts_after_the_allowlist_excludes_a_stored_hdfs_database(started_cluster):
    # No HDFS cluster is needed: `CREATE DATABASE ... ENGINE = HDFS(...)` performs no connection, which
    # is what makes this state easy to reach.
    node.query("CREATE DATABASE hdfs_db ENGINE = HDFS('hdfs://nn.example.invalid:9000')")
    node.query("CREATE TABLE default.bystander (k UInt64) ENGINE = MergeTree ORDER BY k")
    node.query("INSERT INTO default.bystander SELECT number FROM numbers(100)")

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "configs/allowlist.xml"),
        "/etc/clickhouse-server/config.d/allowlist.xml",
    )
    node.restart_clickhouse()

    # The bystander table - and the server - survive the tightening.
    assert node.query("SELECT count() FROM default.bystander") == "100\n"
    assert node.query("SELECT count() FROM system.databases WHERE name = 'hdfs_db'") == "1\n"

    # The allowlist still holds for every use of the database, now at use time rather than at startup:
    # resolution asks `isTableExist`, which refuses the disallowed host, so the database exposes no
    # table for it, and a path that resolves the table directly names the URL.
    error = node.query_and_get_error("SELECT count() FROM hdfs_db.`some_file.tsv`")
    assert "UNKNOWN_TABLE" in error, error
    error = node.query_and_get_error("INSERT INTO hdfs_db.`some_file.tsv` VALUES (1)")
    assert "UNACCEPTABLE_URL" in error, error

    # A definition the user introduces now is still refused up front.
    error = node.query_and_get_error("CREATE DATABASE hdfs_db2 ENGINE = HDFS('hdfs://nn.example.invalid:9000')")
    assert "UNACCEPTABLE_URL" in error, error
    assert node.query("SELECT count() FROM system.databases WHERE name = 'hdfs_db2'") == "0\n"

    node.query("DROP DATABASE hdfs_db SYNC")
    node.query("DROP TABLE default.bystander SYNC")
