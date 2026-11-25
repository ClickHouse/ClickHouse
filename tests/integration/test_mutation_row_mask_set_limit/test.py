import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/background_profile.xml"],
    user_configs=["configs/limited_profile.xml"],
)
replicated_node = cluster.add_instance("replicated_node", with_zookeeper=True)
expanded_ast_node = cluster.add_instance(
    "expanded_ast_node",
    main_configs=["configs/expanded_ast_profile.xml"],
    user_configs=["configs/expanded_ast_users.xml"],
)


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
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "min_bytes_for_full_part_storage = 0"
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
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "min_bytes_for_full_part_storage = 0"
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


def test_mixed_project_updates_respect_background_expanded_ast_limit():
    expanded_ast_node.query(
        "CREATE TABLE t_expanded_ast (n UInt32, project_id UInt32, run_id UInt128) "
        "ENGINE = MergeTree ORDER BY n "
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "min_bytes_for_full_part_storage = 0"
    )
    expanded_ast_node.query(
        "INSERT INTO t_expanded_ast SELECT number + 1, number + 1, number + 1 "
        "FROM numbers(11)"
    )
    expanded_ast_node.query(
        "ALTER TABLE t_expanded_ast ADD INDEX idx project_id TYPE minmax GRANULARITY 1"
    )
    assert expanded_ast_node.query(
        "SELECT part_type, part_storage_type FROM system.parts "
        "WHERE database = currentDatabase() AND table = 't_expanded_ast' AND active"
    ) == "Wide\tFull\n"

    commands = [
        "UPDATE _row_exists = 0 WHERE project_id = "
        f"{n} AND run_id IN (SELECT toUInt128(arrayJoin(['{n}'])))"
        for n in range(1, 11)
    ]
    commands.append("MATERIALIZE INDEX idx")
    expanded_ast_node.query(
        "ALTER TABLE t_expanded_ast "
        + ", ".join(commands)
        + " SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1"
    )
    assert expanded_ast_node.query("SELECT n FROM t_expanded_ast ORDER BY n") == "11\n"
    assert expanded_ast_node.query(
        "SELECT count(), countIf(is_done), countIf(latest_fail_reason != '') "
        "FROM system.mutations WHERE database = currentDatabase() "
        "AND table = 't_expanded_ast'"
    ) == "11\t11\t0\n"
    assert not expanded_ast_node.contains_in_log("Coalesced row-mask updates for part")


def test_separate_replicated_mutations_coalesce_during_part_rewrite():
    replicated_node.query(
        "CREATE TABLE t_replicated "
        "(n UInt64, project_id UInt32, run_id UInt128, payload UInt8) "
        "ENGINE = ReplicatedMergeTree("
        "'/clickhouse/tables/test_mutation_row_mask_set_limit/t_replicated', 'r1') "
        "ORDER BY n SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "min_bytes_for_full_part_storage = 0, "
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


def test_row_mask_coalescing_table_settings():
    replicated_node.query(
        "CREATE TABLE t_coalescing_settings "
        "(n UInt32, project_id UInt32, id String) "
        "ENGINE = MergeTree ORDER BY n "
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "min_bytes_for_full_part_storage = 0, "
        "enable_row_mask_update_coalescing = 0"
    )
    replicated_node.query(
        "INSERT INTO t_coalescing_settings VALUES "
        "(1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd'), "
        "(5, 1, 'e'), (6, 1, 'f'), (7, 1, 'g'), (8, 1, 'h'), "
        "(9, 1, 'i'), (10, 1, 'j'), (11, 1, 'k'), (12, 1, 'l'), (13, 1, 'm')"
    )

    def delete_pair(first, second):
        replicated_node.query(
            "ALTER TABLE t_coalescing_settings "
            f"UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('{first}'), "
            f"UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('{second}') "
            "SETTINGS mutations_sync = 2"
        )

    coalesced_count = int(replicated_node.count_in_log("Coalesced row-mask updates for part"))
    delete_pair("a", "b")
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count

    replicated_node.query(
        "ALTER TABLE t_coalescing_settings MODIFY SETTING enable_row_mask_update_coalescing = 1"
    )
    delete_pair("c", "d")
    coalesced_count += 1
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count

    replicated_node.query(
        "ALTER TABLE t_coalescing_settings MODIFY SETTING max_row_mask_update_coalescing_keys = 1"
    )
    delete_pair("e", "f")
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count

    replicated_node.query(
        "ALTER TABLE t_coalescing_settings MODIFY SETTING "
        "max_row_mask_update_coalescing_keys = 2, max_row_mask_update_coalescing_key_bytes = 1"
    )
    delete_pair("g", "h")
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count

    replicated_node.query(
        "ALTER TABLE t_coalescing_settings MODIFY SETTING "
        "max_row_mask_update_coalescing_key_bytes = 2, max_row_mask_update_coalescing_commands = 1"
    )
    delete_pair("i", "j")
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count

    replicated_node.query(
        "ALTER TABLE t_coalescing_settings MODIFY SETTING "
        "max_row_mask_update_coalescing_commands = 2, max_row_mask_update_coalescing_ast_bytes = 1"
    )
    delete_pair("k", "l")
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count
    assert replicated_node.query("SELECT n, id FROM t_coalescing_settings ORDER BY n") == "13\tm\n"


def test_nested_alias_is_not_coalesced():
    coalesced_count = int(replicated_node.count_in_log("Coalesced row-mask updates for part"))
    for enabled in (0, 1):
        table = f"t_alias_coalescing_{enabled}"
        replicated_node.query(
            f"CREATE TABLE {table} (n UInt32, project_id UInt32, id String) "
            "ENGINE = MergeTree ORDER BY n "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
            "min_bytes_for_full_part_storage = 0, "
            f"enable_row_mask_update_coalescing = {enabled}"
        )
        replicated_node.query(
            f"INSERT INTO {table} VALUES "
            "(1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 2, 'd')"
        )
        assert replicated_node.query(
            "SELECT part_type, part_storage_type FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active"
        ) == "Wide\tFull\n"
        assert replicated_node.query(
            f"SELECT n FROM {table} WHERE project_id = 1 "
            "AND id IN (('a' AS id)) ORDER BY n"
        ) == "1\n2\n3\n"
        replicated_node.query(
            f"ALTER TABLE {table} "
            "UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN (('a' AS id)), "
            "UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN (('a' AS id)) "
            "SETTINGS mutations_sync = 2"
        )
        assert replicated_node.query(f"SELECT n, id FROM {table} ORDER BY n") == "4\td\n"
        assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count


def test_assignment_alias_is_not_coalesced():
    coalesced_count = int(replicated_node.count_in_log("Coalesced row-mask updates for part"))
    for enabled in (0, 1):
        table = f"t_assignment_alias_coalescing_{enabled}"
        replicated_node.query(
            f"CREATE TABLE {table} (n UInt32, project_id UInt32, id String) "
            "ENGINE = MergeTree ORDER BY n "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
            "min_bytes_for_full_part_storage = 0, "
            f"enable_row_mask_update_coalescing = {enabled}"
        )
        replicated_node.query(
            f"INSERT INTO {table} VALUES "
            "(1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 2, 'd')"
        )
        assert replicated_node.query(
            "SELECT part_type, part_storage_type FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active"
        ) == "Wide\tFull\n"
        replicated_node.query(
            f"ALTER TABLE {table} "
            "UPDATE _row_exists = 0 WHERE project_id = 1 AND id IN ('a'), "
            "UPDATE _row_exists = (0 AS project_id) WHERE project_id = 1 AND id IN ('b') "
            "SETTINGS mutations_sync = 2, prefer_column_name_to_alias = 0"
        )
        assert replicated_node.query(f"SELECT n, id FROM {table} ORDER BY n") == "2\tb\n3\tc\n4\td\n"
        assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count


def test_same_project_uint128_subqueries_coalesce():
    replicated_node.query(
        "CREATE TABLE t_same_project_subqueries (n UInt32, project_id UInt32, run_id UInt128) "
        "ENGINE = MergeTree ORDER BY n "
        "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
        "min_bytes_for_full_part_storage = 0"
    )
    replicated_node.query(
        "INSERT INTO t_same_project_subqueries VALUES (1, 1, 1), (2, 1, 2), (3, 1, 3)"
    )
    assert replicated_node.query(
        "SELECT part_type, part_storage_type FROM system.parts "
        "WHERE database = currentDatabase() AND table = 't_same_project_subqueries' AND active"
    ) == "Wide\tFull\n"

    coalesced_count = int(replicated_node.count_in_log("Coalesced row-mask updates for part"))
    replicated_node.query(
        "ALTER TABLE t_same_project_subqueries "
        "UPDATE _row_exists = 0 WHERE project_id = 1 "
        "AND run_id IN (SELECT toUInt128(arrayJoin(['1']))), "
        "UPDATE _row_exists = 0 WHERE project_id = 1 "
        "AND run_id IN (SELECT toUInt128(arrayJoin(['2']))) "
        "SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1"
    )
    assert replicated_node.query(
        "SELECT n, run_id FROM t_same_project_subqueries ORDER BY n"
    ) == "3\t3\n"
    assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count + 1


def test_rollup_subquery_is_not_coalesced():
    coalesced_count = int(replicated_node.count_in_log("Coalesced row-mask updates for part"))
    for enabled in (0, 1):
        table = f"t_rollup_coalescing_{enabled}"
        replicated_node.query(
            f"CREATE TABLE {table} (n UInt32, project_id UInt32, run_id UInt128) "
            "ENGINE = MergeTree ORDER BY n "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
            "min_bytes_for_full_part_storage = 0, "
            f"enable_row_mask_update_coalescing = {enabled}"
        )
        replicated_node.query(
            f"INSERT INTO {table} VALUES (1, 1, 0), (2, 1, 1), (3, 2, 2)"
        )
        assert replicated_node.query(
            "SELECT part_type, part_storage_type FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active"
        ) == "Wide\tFull\n"
        assert replicated_node.query(
            f"SELECT count() FROM {table} WHERE project_id = 1 "
            "AND run_id IN (SELECT toUInt128(arrayJoin(['1'])) GROUP BY ALL WITH ROLLUP) "
            "SETTINGS group_by_use_nulls = 0"
        ) == "2\n"
        replicated_node.query(
            f"ALTER TABLE {table} "
            "UPDATE _row_exists = 0 WHERE project_id = 1 AND run_id IN "
            "(SELECT toUInt128(arrayJoin(['1'])) GROUP BY ALL WITH ROLLUP), "
            "UPDATE _row_exists = 0 WHERE project_id = 2 AND run_id IN "
            "(SELECT toUInt128(arrayJoin(['2']))) "
            "SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1, "
            "group_by_use_nulls = 0"
        )
        assert replicated_node.query(f"SELECT n FROM {table} ORDER BY n") == ""
        assert int(replicated_node.count_in_log("Coalesced row-mask updates for part")) == coalesced_count
