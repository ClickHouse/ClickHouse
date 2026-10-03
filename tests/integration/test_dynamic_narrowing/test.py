"""
Integration test for Dynamic column per-part type narrowing.

Tests:
1. Server restart with narrowed parts on disk
2. Replicated table: write on one node, restart the other, verify data
3. Merge after restart
4. JSON column with partitioned data survives restart
"""

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[],
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


def test_restart_with_narrowed_parts(started_cluster):
    """Write narrowed Dynamic parts, restart server, verify data survives."""
    node1.query(
        """
        CREATE TABLE t_narrow_restart (id UInt64, value Dynamic)
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4'
        """
    )

    # Insert homogeneous data — triggers narrowing
    node1.query(
        "INSERT INTO t_narrow_restart SELECT number, number::Int64 FROM numbers(5000)"
    )
    assert node1.query("SELECT count() FROM t_narrow_restart").strip() == "5000"
    assert node1.query("SELECT min(value::Int64) FROM t_narrow_restart").strip() == "0"
    assert (
        node1.query("SELECT max(value::Int64) FROM t_narrow_restart").strip() == "4999"
    )

    # Hard restart (kill process, start fresh)
    node1.restart_clickhouse(kill=True)

    # Verify data after restart
    assert node1.query("SELECT count() FROM t_narrow_restart").strip() == "5000"
    assert node1.query("SELECT min(value::Int64) FROM t_narrow_restart").strip() == "0"
    assert (
        node1.query("SELECT max(value::Int64) FROM t_narrow_restart").strip() == "4999"
    )

    node1.query("DROP TABLE t_narrow_restart")


def test_restart_with_nullable_narrowing(started_cluster):
    """Narrowed parts with NULLs (Nullable storage) survive restart."""
    node1.query(
        """
        CREATE TABLE t_narrow_nullable (id UInt64, value Dynamic)
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4'
        """
    )

    # 70% data, 30% NULLs
    node1.query(
        "INSERT INTO t_narrow_nullable "
        "SELECT number, if(number % 10 < 3, NULL, number::Int64) FROM numbers(10000)"
    )
    count_before = node1.query("SELECT count() FROM t_narrow_nullable").strip()
    nulls_before = node1.query(
        "SELECT countIf(value IS NULL) FROM t_narrow_nullable"
    ).strip()
    non_nulls_before = node1.query(
        "SELECT countIf(value IS NOT NULL) FROM t_narrow_nullable"
    ).strip()
    max_before = node1.query(
        "SELECT max(value::Int64) FROM t_narrow_nullable WHERE value IS NOT NULL"
    ).strip()

    node1.restart_clickhouse(kill=True)

    assert node1.query("SELECT count() FROM t_narrow_nullable").strip() == count_before
    assert (
        node1.query("SELECT countIf(value IS NULL) FROM t_narrow_nullable").strip()
        == nulls_before
    )
    assert (
        node1.query(
            "SELECT countIf(value IS NOT NULL) FROM t_narrow_nullable"
        ).strip()
        == non_nulls_before
    )
    assert (
        node1.query(
            "SELECT max(value::Int64) FROM t_narrow_nullable WHERE value IS NOT NULL"
        ).strip()
        == max_before
    )

    node1.query("DROP TABLE t_narrow_nullable")


def test_replicated_narrowing_with_restart(started_cluster):
    """Write narrowed part on node1, replicate to node2, restart node2, verify."""
    for node in [node1, node2]:
        node.query(
            """
            CREATE TABLE t_narrow_repl (id UInt64, value Dynamic)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_narrow_repl', '{}')
            ORDER BY id
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4'
            """.format(
                node.name
            )
        )

    # Write on node1
    node1.query(
        "INSERT INTO t_narrow_repl SELECT number, number::Int64 FROM numbers(3000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_narrow_repl")

    assert node2.query("SELECT count() FROM t_narrow_repl").strip() == "3000"

    # Restart node2 (the replica that received data via replication)
    node2.restart_clickhouse(kill=True)

    # Verify data survives restart on the replica
    assert node2.query("SELECT count() FROM t_narrow_repl").strip() == "3000"
    assert node2.query("SELECT min(value::Int64) FROM t_narrow_repl").strip() == "0"
    assert node2.query("SELECT max(value::Int64) FROM t_narrow_repl").strip() == "2999"

    # Write more data on node2 after restart, merge
    node2.query(
        "INSERT INTO t_narrow_repl SELECT number + 3000, 'str_' || toString(number) FROM numbers(1000)"
    )
    node1.query("SYSTEM SYNC REPLICA t_narrow_repl")
    node1.query("OPTIMIZE TABLE t_narrow_repl FINAL")
    node2.query("SYSTEM SYNC REPLICA t_narrow_repl")

    assert node2.query("SELECT count() FROM t_narrow_repl").strip() == "4000"
    assert (
        node2.query(
            "SELECT count() FROM t_narrow_repl WHERE dynamicType(value) = 'Int64'"
        ).strip()
        == "3000"
    )
    assert (
        node2.query(
            "SELECT count() FROM t_narrow_repl WHERE dynamicType(value) = 'String'"
        ).strip()
        == "1000"
    )

    for node in [node1, node2]:
        node.query("DROP TABLE t_narrow_repl")


def test_json_partitioned_restart(started_cluster):
    """JSON column with PARTITION BY action_type, narrowed parts, restart."""
    node1.query(
        """
        CREATE TABLE t_json_restart (
            id UInt64,
            action_type Int32,
            payload JSON
        )
        ENGINE = MergeTree
        PARTITION BY action_type
        ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4'
        """
    )

    node1.query(
        """INSERT INTO t_json_restart FORMAT JSONEachRow
{"id":1,"action_type":4,"payload":{"battle_type":10,"win":true,"damage":12345}}
{"id":2,"action_type":4,"payload":{"battle_type":5,"win":false,"damage":67890}}
{"id":3,"action_type":3,"payload":{"item_id":100,"count":5}}
{"id":4,"action_type":3,"payload":{"item_id":200,"count":10}}"""
    )

    # Verify before restart
    assert node1.query("SELECT count() FROM t_json_restart").strip() == "4"
    battle = node1.query(
        "SELECT id, payload.battle_type, payload.win FROM t_json_restart "
        "WHERE action_type = 4 ORDER BY id"
    ).strip()

    # Hard restart
    node1.restart_clickhouse(kill=True)

    # Verify after restart
    assert node1.query("SELECT count() FROM t_json_restart").strip() == "4"
    battle_after = node1.query(
        "SELECT id, payload.battle_type, payload.win FROM t_json_restart "
        "WHERE action_type = 4 ORDER BY id"
    ).strip()
    assert battle == battle_after

    items_after = node1.query(
        "SELECT id, payload.item_id, payload.count FROM t_json_restart "
        "WHERE action_type = 3 ORDER BY id"
    ).strip()
    assert "100" in items_after
    assert "200" in items_after

    node1.query("DROP TABLE t_json_restart")


def test_merge_after_restart(started_cluster):
    """Write multiple narrowed parts, restart, then merge — verify correctness."""
    node1.query(
        """
        CREATE TABLE t_merge_restart (id UInt64, value Dynamic)
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, dynamic_serialization_version = 'v4'
        """
    )

    # Three separate parts, each homogeneous (each narrowed)
    node1.query(
        "INSERT INTO t_merge_restart SELECT number, number::Int64 FROM numbers(1000)"
    )
    node1.query(
        "INSERT INTO t_merge_restart SELECT number + 1000, number::Float64 FROM numbers(1000)"
    )
    node1.query(
        "INSERT INTO t_merge_restart SELECT number + 2000, 'str_' || toString(number) FROM numbers(1000)"
    )

    # Restart before merge
    node1.restart_clickhouse(kill=True)

    # Merge after restart
    node1.query("OPTIMIZE TABLE t_merge_restart FINAL")

    assert node1.query("SELECT count() FROM t_merge_restart").strip() == "3000"
    assert (
        node1.query(
            "SELECT count() FROM t_merge_restart WHERE dynamicType(value) = 'Int64'"
        ).strip()
        == "1000"
    )
    assert (
        node1.query(
            "SELECT count() FROM t_merge_restart WHERE dynamicType(value) = 'Float64'"
        ).strip()
        == "1000"
    )
    assert (
        node1.query(
            "SELECT count() FROM t_merge_restart WHERE dynamicType(value) = 'String'"
        ).strip()
        == "1000"
    )

    node1.query("DROP TABLE t_merge_restart")
