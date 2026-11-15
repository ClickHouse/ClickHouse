import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import replace_text_in_metadata

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# A table whose startup failed must not answer a hypothetical object query from a user
# without grants on it: access is checked before the table is looked up.
def test_access_is_checked_before_failed_table_startup(started_cluster):
    node.query(
        """
        CREATE DATABASE test_broken ENGINE = Atomic;
        CREATE TABLE test_broken.t (x UInt64) ENGINE = MergeTree ORDER BY x;
        CREATE USER u_no_grants NOT IDENTIFIED;
        """
    )
    metadata_path = node.query(
        "SELECT metadata_path FROM system.tables "
        "WHERE database = 'test_broken' AND name = 't'"
    ).strip()

    node.stop_clickhouse()
    replace_text_in_metadata(
        node, metadata_path, "ENGINE = MergeTree", "ENGINE = NoSuchEngine"
    )
    node.start_clickhouse()

    error = node.query_and_get_error("SELECT * FROM test_broken.t")
    assert "ASYNC_LOAD_WAIT_FAILED" in error, error

    for query in [
        "CREATE HYPOTHETICAL INDEX i ON test_broken.t (x) TYPE minmax",
        "DROP HYPOTHETICAL INDEX i ON test_broken.t",
        "CREATE HYPOTHETICAL PROJECTION p ON test_broken.t (SELECT * ORDER BY x)",
        "DROP HYPOTHETICAL PROJECTION p ON test_broken.t",
    ]:
        error = node.query_and_get_error(query, user="u_no_grants")
        assert "ACCESS_DENIED" in error, error
        assert "NoSuchEngine" not in error, error
