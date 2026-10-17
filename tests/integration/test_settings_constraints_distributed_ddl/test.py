"""
Test that settings constraints are respected (or at least clamped) when
distributed DDL entries are replayed on worker nodes that have stricter
constraints than the initiator node.

Scenario
--------
- node1 (initiator): NO constraints on max_memory_usage
- node2 (worker):    max_memory_usage constrained to MIN 5_000_000_000

The initiator sets max_memory_usage = 1 in its session and then issues
CREATE TABLE ... ON CLUSTER. The DDL entry serializes max_memory_usage = 1.
When node2 replays this entry, it should ideally clamp or reject the
setting, not silently apply it in violation of local constraints.

We verify by checking the query_log on node2 to see what effective
max_memory_usage was applied when the DDL worker replayed the entry.
"""

import uuid
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# node1: no constraints on max_memory_usage (unconstrained initiator)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    macros={"shard": "1", "replica": "1"},
)

# node2: strict constraint on max_memory_usage (min=5_000_000_000)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/node2_users.xml"],
    with_zookeeper=True,
    macros={"shard": "2", "replica": "1"},
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_ddl_worker_clamps_settings_constraints():
    """
    Verify that the DDL worker on node2 clamps settings constraints
    when replaying a DDL entry from node1 that carries a setting
    violating node2's local constraints.
    """
    table_name = f"test_ddl_constraints_{uuid.uuid4().hex[:8]}"

    # Verify the constraint is active on node2
    assert "shouldn't be less" in node2.query_and_get_error(
        "SET max_memory_usage = 1"
    ), "Constraint should be active on node2"

    # Verify no constraint on node1
    node1.query("SET max_memory_usage = 1")  # Should succeed

    # From node1: run ON CLUSTER DDL with max_memory_usage=1
    # This creates the DDL entry with the setting serialized in it
    node1.query(
        f"CREATE TABLE {table_name} ON CLUSTER test_cluster (x Int64) ENGINE = MergeTree ORDER BY x",
        settings={
            "max_memory_usage": "1",
            "distributed_ddl_entry_format_version": "2",
            "distributed_ddl_task_timeout": "60",
        },
    )

    # Both nodes should have the table
    assert node1.query(f"SELECT count() FROM system.tables WHERE name = '{table_name}'").strip() == "1"
    assert node2.query(f"SELECT count() FROM system.tables WHERE name = '{table_name}'").strip() == "1"

    # Wait for the DDL worker's query to appear in query_log on node2.
    assert_eq_with_retry(
        node2,
        f"SELECT count() FROM system.query_log WHERE query LIKE '%CREATE TABLE%{table_name}%' AND type = 'QueryFinish' AND is_initial_query = 0",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )

    # Now that the entry exists, verify the effective max_memory_usage was
    # clamped to at least the minimum constraint (5_000_000_000).
    result = int(
        node2.query(
            f"SELECT Settings['max_memory_usage'] FROM system.query_log WHERE query LIKE '%CREATE TABLE%{table_name}%' AND type = 'QueryFinish' AND is_initial_query = 0"
        ).strip()
    )
    assert result >= 5_000_000_000, (
        f"DDL worker on node2 applied max_memory_usage={result}, "
        f"which violates the local constraint (min=5000000000)"
    )

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
