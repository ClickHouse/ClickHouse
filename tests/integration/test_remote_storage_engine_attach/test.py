import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_engine_attaches_after_target_dropped(started_cluster):
    # A `Remote` engine over a local-shard table-function target (`merge(...)`) with explicit columns.
    #
    # At `CREATE` time the target function is analyzed under the user's context to validate access
    # to its underlying tables. This validation must NOT run again when the table is loaded from its
    # (already validated) metadata on server startup: otherwise dropping the table the target reads
    # from makes the persisted `Remote` table impossible to load, even though it carries its own
    # explicit columns and never needs the target to be analyzed in order to attach.
    node.query("DROP TABLE IF EXISTS src SYNC")
    node.query("DROP TABLE IF EXISTS t_remote SYNC")

    node.query("CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO src VALUES (42)")

    node.query(
        "CREATE TABLE t_remote (x UInt64) ENGINE = Remote('127.0.0.1', merge('default', '^src$'))"
    )
    assert node.query("SELECT x FROM t_remote ORDER BY x").strip() == "42"

    # Drop the table the target function reads from, then restart so the engine is loaded from metadata.
    node.query("DROP TABLE src SYNC")
    node.restart_clickhouse(kill=True)

    # The `Remote` table must still be attached after the restart.
    assert node.query("EXISTS TABLE t_remote").strip() == "1"
    assert "Remote" in node.query("SHOW CREATE TABLE t_remote")

    node.query("DROP TABLE IF EXISTS t_remote SYNC")
