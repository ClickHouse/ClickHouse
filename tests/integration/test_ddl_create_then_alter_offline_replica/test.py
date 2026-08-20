"""
Test for GitHub issue https://github.com/ClickHouse/ClickHouse/issues/44070:
When one replica is down and another executes CREATE TABLE ... ON CLUSTER followed by ALTER TABLE ADD COLUMN ... ON CLUSTER, 
the offline replica fails to create the table when it comes back up. The DDLWorker processes the CREATE TABLE with the original columns, 
but ZooKeeper already has the altered schema (with the extra column). This causes an INCOMPATIBLE_COLUMNS exception,
and then the ALTER also fails because the table was never created.
"""

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "1", "replica": "node1"},
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    macros={"shard": "1", "replica": "node2"},
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _collect_diagnostics(node, table_name):
    """Collect DDL queue and log diagnostics from a node for debugging."""
    sections = [
        (
            "=== DDL queue ===",
            "SELECT entry, entry_version, initiator_host, query, status, exception_code, exception_text "
            "FROM system.distributed_ddl_queue "
            f"WHERE query LIKE '%{table_name}%' "
            "ORDER BY entry "
            "FORMAT PrettyCompactMonoBlock",
        ),
        (
            "=== DDLWorker log ===",
            "SELECT message FROM system.text_log "
            "WHERE logger_name LIKE '%DDLWorker%' "
            f"AND message LIKE '%{table_name}%' "
            "ORDER BY event_time_microseconds "
            "LIMIT 100 "
            "FORMAT PrettyCompactMonoBlock",
        ),
    ]
    parts = []
    for header, sql in sections:
        parts.append(header)
        try:
            parts.append(node.query(sql))
        except Exception as e:
            parts.append(f"(query failed: {e})")
    return "\n".join(parts)


# TODO: Once #44070 is fixed, this test will fail. To turn it into a regression test, simply remove the @pytest.mark.xfail decorator.
@pytest.mark.xfail(
    strict=True,
    reason="Known bug #44070: DDLWorker fails with INCOMPATIBLE_COLUMNS when replaying CREATE TABLE after concurrent ALTER",
)
def test_create_then_alter_with_offline_replica(started_cluster):
    """
    Reproduces the bug where CREATE TABLE ON CLUSTER + ALTER ADD COLUMN ON CLUSTER while a replica is offline causes INCOMPATIBLE_COLUMNS 
    on the offline replica when it comes back up.
    """
    table_name = "test_issue_44070"
    zk_path = "/clickhouse/tables/{shard}/test_issue_44070"
    expected_columns = "ID\nx01\nx02\ntime"
    columns_query = (
        f"SELECT name FROM system.columns "
        f"WHERE database = 'default' AND table = '{table_name}' ORDER BY position"
    )

    # Low timeout and never_throw so that DDL ON CLUSTER returns quickly without an error while node2 is offline.
    ddl_settings = {
        "distributed_ddl_task_timeout": 5,
        "distributed_ddl_output_mode": "never_throw",
    }

    # Cleanup from any previous run
    node1.query(
        f"DROP TABLE IF EXISTS default.{table_name} ON CLUSTER test_cluster SYNC",
        settings=ddl_settings,
    )

    # Stop node2 before issuing DDL
    node2.stop_clickhouse()

    # CREATE TABLE and ALTER TABLE ADD COLUMN on cluster while node2 is down
    node1.query(
        f"""
        CREATE TABLE default.{table_name} ON CLUSTER test_cluster
        (ID Int64, x01 String, x02 String)
        ENGINE = ReplicatedMergeTree('{zk_path}', '{{replica}}')
        ORDER BY ID
        """,
        settings=ddl_settings,
    )

    node1.query(
        f"""
        ALTER TABLE default.{table_name} ON CLUSTER test_cluster
        ADD COLUMN time Int64
        """,
        settings=ddl_settings,
    )

    # Verify node1 has the expected columns
    assert node1.query(columns_query).strip() == expected_columns

    # Bring node2 back up and wait for DDLWorker to process both entries
    node2.start_clickhouse()

    try:
        assert_eq_with_retry(
            node2,
            columns_query,
            expected_columns,
            retry_count=30,
            sleep_time=1,
        )
    except AssertionError:
        diagnostics = _collect_diagnostics(node2, table_name)
        pytest.fail(
            f"node2 did not get the expected columns in default.{table_name}.\n"
            f"{diagnostics}"
        )

    # Verify data replication works
    node1.query(f"INSERT INTO default.{table_name} VALUES (1, 'a', 'b', 100)")
    node2.query(f"SYSTEM SYNC REPLICA default.{table_name}", timeout=30)
    assert (
        node2.query(f"SELECT ID, x01, x02, time FROM default.{table_name}").strip()
        == "1\ta\tb\t100"
    )
