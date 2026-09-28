import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/background_profile.xml"],
    user_configs=["configs/limited_profile.xml"],
)
replicated_node = cluster.add_instance("replicated_node", with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_singleton_row_mask_updates_respect_background_set_limit():
    node.query(
        "CREATE TABLE t (n UInt32, project_id UInt32, id String) "
        "ENGINE = MergeTree ORDER BY n "
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
    )
    node.query("INSERT INTO t VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c')")

    # Both singleton sets fit the background limit; a combined set would not.
    node.query(
        "ALTER TABLE t "
        "UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'), "
        "UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('b') "
        "SETTINGS mutations_sync = 2"
    )

    assert node.query("SELECT n, id FROM t ORDER BY n") == "3\tc\n"


def test_mixed_project_subquery_updates_respect_background_set_limit():
    node.query(
        "CREATE TABLE t_subquery (n UInt32, project_id UInt32, run_id UInt128) "
        "ENGINE = MergeTree ORDER BY n "
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
    )
    node.query(
        "INSERT INTO t_subquery VALUES "
        "(1, 1, 1), (2, 2, 2), (3, 1, 2), (4, 2, 1)"
    )

    # Each one-row set fits. A tuple set with two project/run pairs does not.
    node.query(
        "ALTER TABLE t_subquery "
        "UPDATE _row_exists = 0 WHERE (project_id = 1) "
        "AND (run_id IN (SELECT toUInt128(arrayJoin(['1'])))), "
        "UPDATE _row_exists = 0 WHERE (project_id = 2) "
        "AND (run_id IN (SELECT toUInt128(arrayJoin(['2'])))) "
        "SETTINGS mutations_sync = 2"
    )

    assert node.query("SELECT n, project_id, run_id FROM t_subquery ORDER BY n") == "3\t1\t2\n4\t2\t1\n"


def test_separate_replicated_mutations_coalesce_during_part_rewrite():
    replicated_node.query(
        "CREATE TABLE t_replicated "
        "(n UInt64, project_id UInt32, run_id UInt128, payload UInt8) "
        "ENGINE = ReplicatedMergeTree("
        "'/clickhouse/tables/test_mutation_row_mask_set_limit/t_replicated', 'r1') "
        "ORDER BY n SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "number_of_free_entries_in_pool_to_execute_mutation = 0, "
        "number_of_free_entries_in_pool_to_execute_optimize_entire_partition = 0"
    )
    replicated_node.query(
        "INSERT INTO t_replicated VALUES "
        "(1, 1, 1, 0), (2, 1, 2, 0), (3, 1, 3, 0), "
        "(4, 2, 1, 0), (5, 2, 2, 0), (6, 2, 3, 0), (7, 3, 1, 0)"
    )
    replicated_node.query("SYSTEM STOP MERGES t_replicated")
    for project, key in [(1, 1), (2, 1), (1, 2)]:
        replicated_node.query(
            "ALTER TABLE t_replicated UPDATE _row_exists = 0 "
            f"WHERE project_id = {project} AND "
            f"run_id IN (SELECT toUInt128(arrayJoin(['{key}']))) "
            "SETTINGS mutations_sync = 0, allow_nondeterministic_mutations = 1"
        )
    replicated_node.query(
        "ALTER TABLE t_replicated UPDATE payload = 9 WHERE n = 6 "
        "SETTINGS mutations_sync = 0"
    )
    for project, key in [(1, 3), (2, 2)]:
        replicated_node.query(
            "ALTER TABLE t_replicated UPDATE _row_exists = 0 "
            f"WHERE project_id = {project} AND "
            f"run_id IN (SELECT toUInt128(arrayJoin(['{key}']))) "
            "SETTINGS mutations_sync = 0, allow_nondeterministic_mutations = 1"
        )

    assert replicated_node.query(
        "SELECT count() FROM system.mutations "
        "WHERE database = currentDatabase() AND table = 't_replicated' AND NOT is_done"
    ) == "6\n"

    replicated_node.query("SYSTEM START MERGES t_replicated")
    replicated_node.query(
        "ALTER TABLE t_replicated UPDATE payload = payload WHERE n = 7 "
        "SETTINGS mutations_sync = 2"
    )
    assert replicated_node.query(
        "SELECT n, project_id, run_id, payload FROM t_replicated ORDER BY n"
    ) == "6\t2\t3\t9\n7\t3\t1\t0\n"
    assert replicated_node.query(
        "SELECT count(), countIf(is_done) FROM system.mutations "
        "WHERE database = currentDatabase() AND table = 't_replicated'"
    ) == "7\t7\n"
    assert replicated_node.contains_in_log("Coalesced row-mask updates for part")
