import pytest
import random
import time

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
)

node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("db_engine", ["Replicated"])
@pytest.mark.parametrize("table_engine", ["ReplicatedMergeTree"])
def test_lwu_replicated_database(started_cluster, db_engine, table_engine):
    db_name = "lwu_db" + str(random.randint(0, 10000000))
    settings = {
        "enable_lightweight_update": 1,
        "lightweight_delete_mode": "lightweight_update_force",
    }

    node1.query(f"DROP DATABASE IF EXISTS {db_name}")
    node2.query(f"DROP DATABASE IF EXISTS {db_name}")

    if db_engine == "Replicated":
        node1.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'r1')"
        )
        node2.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('/test/{db_name}', 'shard1', 'r2')"
        )
    else:
        node1.query(f"CREATE DATABASE {db_name} ENGINE = {db_engine}")

    node1.query(
        f"""
        CREATE TABLE {db_name}.lwu_table (x Int32, y String) ENGINE = {table_engine} ORDER BY x
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1"""
    )

    node1.query(f"INSERT INTO {db_name}.lwu_table VALUES (1, 'a'), (2, 'b') (3, 'c')")

    node1.query(f"DELETE FROM {db_name}.lwu_table WHERE x = 2", settings=settings)
    node1.query(
        f"UPDATE {db_name}.lwu_table SET y = 'updated' WHERE x = 1", settings=settings
    )

    if db_engine == "Replicated":
        node2.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")

    node2.query(f"SYSTEM SYNC REPLICA {db_name}.lwu_table")

    expected = "1\tupdated\n3\tc\n"

    assert TSV(node1.query(f"SELECT * FROM {db_name}.lwu_table")) == TSV(expected)
    assert TSV(node2.query(f"SELECT * FROM {db_name}.lwu_table")) == TSV(expected)

    node1.query("SYSTEM FLUSH LOGS")
    node2.query("SYSTEM FLUSH LOGS")

    # Check that queries were excecuted only on one replica.
    expected = f"DELETE FROM {db_name}.lwu_table WHERE x = 2\nUPDATE {db_name}.lwu_table SET y = \\'updated\\' WHERE x = 1\n"

    assert (
        node1.query(
            f"""
        SELECT query FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind IN ('Update', 'Delete') AND has(databases, '{db_name}')
        ORDER BY event_time_microseconds
    """
        )
        == expected
    )

    assert (
        node2.query(
            f"""
        SELECT query FROM system.query_log
        WHERE type = 'QueryFinish' AND query_kind IN ('Update', 'Delete') AND has(databases, '{db_name}')
        ORDER BY event_time_microseconds
    """
        )
        == ""
    )


def test_lwu_replicated_mutation_pins_patches(started_cluster):
    # Regression test for issue #100493: a replica must apply exactly the same set of patch parts
    # (from lightweight updates) as the replica that assigned the mutation. Otherwise two replicas
    # executing the same MUTATE_PART entry could materialize byte-different parts from divergent
    # local patch state, raising CHECKSUM_DOESNT_MATCH and getting stuck fetching the part.
    node1.query("DROP TABLE IF EXISTS t_lwu_pin SYNC")
    node2.query("DROP TABLE IF EXISTS t_lwu_pin SYNC")

    # lightweight_deletes_sync must be 1, not the default 2: a DELETE runs as a heavyweight
    # mutation (lightweight_delete_mode defaults to alter_update) whose sync mode is taken from
    # lightweight_deletes_sync, overriding mutations_sync. With the default 2 the DELETE on node1
    # would synchronously wait for node2 to apply the mutation, but node2 has fetches stopped on
    # purpose, so it would hang until the query timeout. 1 waits only for the local replica.
    settings = {
        "enable_lightweight_update": 1,
        "mutations_sync": 0,
        "lightweight_deletes_sync": 1,
    }

    for node, replica in [(node1, "r1"), (node2, "r2")]:
        node.query(
            f"""
            CREATE TABLE t_lwu_pin (id UInt64, a UInt64, b UInt64)
            ENGINE = ReplicatedMergeTree('/test/t_lwu_pin', '{replica}')
            ORDER BY id
            SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                     apply_patches_on_merge = 0
            """
        )

    node1.query("INSERT INTO t_lwu_pin SELECT number, number, number FROM numbers(10000)")
    node2.query("SYSTEM SYNC REPLICA t_lwu_pin")

    # Delay patch-part replication to node2 so that, at the moment a mutation is assigned, the two
    # replicas have different sets of visible patch parts. This is exactly the window that used to
    # produce divergent mutated parts.
    node2.query("SYSTEM STOP FETCHES t_lwu_pin")

    # Mixed lightweight UPDATE + DELETE (both required to trigger the original divergence).
    node1.query(
        "UPDATE t_lwu_pin SET b = b + 1 WHERE id >= 1000 AND id < 2000", settings=settings
    )
    node1.query("DELETE FROM t_lwu_pin WHERE id >= 5000 AND id < 6000", settings=settings)
    node1.query(
        "UPDATE t_lwu_pin SET a = a + 7 WHERE id >= 1500 AND id < 2500", settings=settings
    )

    # Force materialization of the patches into a mutated part on the assigning replica.
    node1.query("ALTER TABLE t_lwu_pin APPLY PATCHES IN PARTITION tuple()")
    node1.query("SYSTEM SYNC REPLICA t_lwu_pin", settings={"receive_timeout": 60})

    # Let node2 catch up and execute the same mutation. With the fix it waits for the pinned patch
    # parts and produces a byte-identical result; without the fix it would loop on CHECKSUM_DOESNT_MATCH.
    node2.query("SYSTEM START FETCHES t_lwu_pin")
    node2.query("SYSTEM SYNC REPLICA t_lwu_pin", settings={"receive_timeout": 60})

    expected = node1.query("SELECT id, a, b FROM t_lwu_pin ORDER BY id")
    assert TSV(node2.query("SELECT id, a, b FROM t_lwu_pin ORDER BY id")) == TSV(expected)

    # No replica should have gotten stuck on a checksum mismatch while executing the mutation.
    for node in (node1, node2):
        assert not node.contains_in_log(
            "Data after mutation is not byte-identical"
        ), f"unexpected checksum mismatches on {node.name}"

    node1.query("DROP TABLE t_lwu_pin SYNC")
    node2.query("DROP TABLE IF EXISTS t_lwu_pin SYNC")


def test_lwu_replicated_mutation_accepts_covering_patch(started_cluster):
    # Regression test for the covering-patch race in issue #100493. A MUTATE_PART entry pins the exact
    # names of the patch parts it applies. The queue blocker only stops a patch-part merge that is
    # still queued; a merge that already committed on one replica before the mutation was assigned can
    # replace the pinned names by a single covering merged patch. Rejecting by exact name would then
    # make that replica fall back to fetch a mutated part nobody produced, deadlocking the queue.
    # MutateFromLogEntryTask must accept the covering merged patch instead.
    #
    # The divergence is set up deterministically: node1's patch merge is paused so node1 keeps the two
    # individual patch parts and pins them into the mutation, while node2 completes the same merge and
    # ends up with only the covering merged patch when it materializes the mutation locally.
    node1.query("DROP TABLE IF EXISTS t_lwu_cover SYNC")
    node2.query("DROP TABLE IF EXISTS t_lwu_cover SYNC")

    settings = {
        "enable_lightweight_update": 1,
        "mutations_sync": 0,
    }

    for node, replica in [(node1, "r1"), (node2, "r2")]:
        node.query(
            f"""
            CREATE TABLE t_lwu_cover (id UInt64, a UInt64, b UInt64)
            ENGINE = ReplicatedMergeTree('/test/t_lwu_cover', '{replica}')
            ORDER BY id
            SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
                     apply_patches_on_merge = 0
            """
        )

    node1.query("INSERT INTO t_lwu_cover SELECT number, number, number FROM numbers(10000)")
    node2.query("SYSTEM SYNC REPLICA t_lwu_cover")

    # Two lightweight updates in the same mutation-version bucket produce two separate patch parts.
    node1.query("UPDATE t_lwu_cover SET b = b + 1 WHERE id >= 1000 AND id < 2000", settings=settings)
    node1.query("UPDATE t_lwu_cover SET a = a + 7 WHERE id >= 1500 AND id < 2500", settings=settings)
    node2.query("SYSTEM SYNC REPLICA t_lwu_cover")

    patch_partition = node2.query(
        "SELECT partition_id FROM system.parts WHERE table = 't_lwu_cover' AND active "
        "AND startsWith(partition_id, 'patch') LIMIT 1"
    ).strip()
    assert patch_partition, "expected a patch partition"

    # Pause the patch merge on node1 and stop its fetches so node1 keeps both individual patch parts
    # active (and therefore pins them when it assigns the mutation below).
    node1.query("SYSTEM STOP FETCHES t_lwu_cover")
    node1.query("SYSTEM ENABLE FAILPOINT rmt_merge_task_pause_in_prepare")

    # Merge the two patch parts on node2 into one covering patch. This creates a replicated MERGE entry
    # that node2 executes to completion (node1's copy of it stays paused), so node2 ends up with only
    # the covering merged patch and the two pinned names become inactive there.
    node2.query(
        f"OPTIMIZE TABLE t_lwu_cover PARTITION ID '{patch_partition}' FINAL",
        settings={"optimize_throw_if_noop": 1, "alter_sync": 1},
    )

    # Stop fetches on node2 too, so it must materialize the mutation locally (the code path that
    # resolves the pinned patch parts) instead of downloading node1's mutated part.
    node2.query("SYSTEM STOP FETCHES t_lwu_cover")

    # Assign and materialize the mutation on node1: it still sees both individual patch parts (its merge
    # is paused), so the MUTATE_PART entry pins their exact names.
    node1.query("ALTER TABLE t_lwu_cover APPLY PATCHES IN PARTITION tuple()")

    # node2 executes the same MUTATE_PART entry. The pinned names are gone there, only the covering
    # merged patch is active. With the fix node2 accepts it and materializes a byte-identical mutated
    # part; without the fix it rejects by exact name and loops trying to fetch a part nobody produced.
    def mutated_part_ready(node):
        return (
            node.query(
                "SELECT count() FROM system.parts WHERE table = 't_lwu_cover' AND active "
                "AND name = 'all_0_0_0_3'"
            ).strip()
            == "1"
        )

    for _ in range(600):
        if mutated_part_ready(node2):
            break
        time.sleep(0.1)
    assert mutated_part_ready(node2), "node2 did not materialize the mutated part from the covering patch"

    # Resume node1's paused merge and let both replicas settle, then check they converged.
    node1.query("SYSTEM DISABLE FAILPOINT rmt_merge_task_pause_in_prepare")
    node1.query("SYSTEM START FETCHES t_lwu_cover")
    node2.query("SYSTEM START FETCHES t_lwu_cover")
    node1.query("SYSTEM SYNC REPLICA t_lwu_cover", settings={"receive_timeout": 60})
    node2.query("SYSTEM SYNC REPLICA t_lwu_cover", settings={"receive_timeout": 60})

    expected = node1.query("SELECT id, a, b FROM t_lwu_cover ORDER BY id")
    assert TSV(node2.query("SELECT id, a, b FROM t_lwu_cover ORDER BY id")) == TSV(expected)

    node1.query("DROP TABLE t_lwu_cover SYNC")
    node2.query("DROP TABLE IF EXISTS t_lwu_cover SYNC")


@pytest.mark.parametrize("table_engine", ["ReplicatedMergeTree"])
def test_lwu_upgrade(started_cluster, table_engine):
    node3.query("DROP TABLE IF EXISTS lwu_table_upgrade SYNC")

    if CLICKHOUSE_CI_MIN_TESTED_VERSION not in node3.query("select version()").strip():
        node3.restart_with_original_version(clear_data_dir=True)

    node3.query(
        f"CREATE TABLE lwu_table_upgrade (x Int32, y String) ENGINE = {table_engine}('/test/clickhouse/default/lwu_table_upgrade', '1') ORDER BY x"
    )
    node3.query(
        "INSERT INTO lwu_table_upgrade SELECT number, 'v' || toString(number) FROM numbers(100000)"
    )
    node3.query(
        "INSERT INTO lwu_table_upgrade SELECT number, 'v' || toString(number) FROM numbers(100000, 100000)"
    )

    node3.query("OPTIMIZE TABLE lwu_table_upgrade FINAL")

    with pytest.raises(Exception) as e:
        node3.query(
            "UPDATE lwu_table_upgrade SET y = 'updated' WHERE x >= 50000 AND x < 150000"
        )
    assert "SYNTAX_ERROR" in str(e.value)

    node3.restart_with_latest_version()

    with pytest.raises(Exception) as e:
        node3.query(
            "UPDATE lwu_table_upgrade SET y = 'updated' WHERE x >= 50000 AND x < 150000",
            settings={
                "enable_lightweight_update": 1,
                "update_parallel_mode": "auto",
            },
        )
    assert "NOT_IMPLEMENTED" in str(e.value)

    node3.query(
        "ALTER TABLE lwu_table_upgrade MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0"
    )
    node3.query(
        "UPDATE lwu_table_upgrade SET y = 'updated' WHERE x >= 50000 AND x < 150000",
        settings={
            "enable_lightweight_update": 1,
            "update_parallel_mode": "auto",
        },
    )

    assert (
        node3.query("SELECT count() FROM lwu_table_upgrade WHERE y = 'updated'")
        == "100000\n"
    )

    node3.query("OPTIMIZE TABLE lwu_table_upgrade FINAL")

    assert (
        node3.query("SELECT count() FROM lwu_table_upgrade WHERE y = 'updated'")
        == "100000\n"
    )

    node3.query(
        "ALTER TABLE lwu_table_upgrade MODIFY SETTING apply_patches_on_merge = 1"
    )

    node3.query("OPTIMIZE TABLE lwu_table_upgrade FINAL")

    assert (
        node3.query(
            "SELECT count() FROM lwu_table_upgrade WHERE y = 'updated'",
            settings={"apply_patch_parts": 0},
        )
        == "100000\n"
    )


def test_lwu_on_cluster(started_cluster):
    node1.query("DROP TABLE IF EXISTS t_lwu_on_cluster")
    node2.query("DROP TABLE IF EXISTS t_lwu_on_cluster")

    create_query = """
    CREATE TABLE t_lwu_on_cluster
    (
        `id` UInt32,
        `value` String,
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1
"""

    node1.query(create_query)
    node2.query(create_query)

    node1.query(
        "INSERT INTO t_lwu_on_cluster SELECT number, '' FROM numbers(10000) WHERE number % 4 != 0"
    )
    node2.query(
        "INSERT INTO t_lwu_on_cluster SELECT number, '' FROM numbers(10000) WHERE number % 4 != 1"
    )

    assert (
        node1.query(
            "SELECT count() from remote(test_cluster, currentDatabase(), t_lwu_on_cluster)"
        )
        == "15000\n"
    )
    node1.query(
        "UPDATE t_lwu_on_cluster ON CLUSTER test_cluster SET value = 'updated' WHERE id >= 2000 AND id < 3000"
    )
    assert (
        node1.query(
            "SELECT count() from remote(test_cluster, currentDatabase(), t_lwu_on_cluster) WHERE value = 'updated'"
        )
        == "1500\n"
    )
