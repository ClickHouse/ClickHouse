import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


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


def typed_union_part_log(table, start):
    return (
        "FROM system.part_log WHERE event_date >= yesterday() "
        "AND event_time >= now() - 600 "
        f"AND toUnixTimestamp64Micro(event_time_microseconds) >= {start} "
        f"AND database = currentDatabase() AND table = '{table}' "
        "AND table_uuid = (SELECT uuid FROM system.tables "
        f"WHERE database = currentDatabase() AND name = '{table}') "
        "AND event_type = 'MutatePart'"
    )


def assert_typed_union_part_log(source, predicate):
    assert_eq_with_retry(
        replicated_node,
        f"SYSTEM FLUSH LOGS part_log; SELECT {predicate} {source} "
        "SETTINGS max_execution_time = 2",
        "1",
        retry_count=20,
        timeout=3,
    )


TYPED_UNION_CASES = {
    "scalar": (
        "tenant IN (-1, -1, -2)",
        "tenant IN (1, -1, 2)",
        lambda n: n // 6 != 1,
    ),
    "uint128": (
        "item IN (toUInt128('18446744073709551617'), "
        "toUInt128('18446744073709551617'), toUInt128('18446744073709551620'))",
        "item IN (toUInt128('18446744073709551618'), "
        "toUInt128('18446744073709551617'), toUInt128('18446744073709551620'))",
        lambda n: n % 3 < 2,
    ),
    "tuple": (
        "(tenant, item) IN ((-1, toUInt128('18446744073709551617')), "
        "(-1, toUInt128('18446744073709551617')), (-2, toUInt128('18446744073709551618')))",
        "(tenant, item) IN ((1, toUInt128('18446744073709551618')), "
        "(-1, toUInt128('18446744073709551617')), (2, toUInt128('18446744073709551619')))",
        lambda n: (n // 6 == 0 and n % 3 == 0) or (n // 6 == 2 and n % 3 == 1),
    ),
    "computed": (
        "toUInt64(item) IN (1, 99)",
        "toUInt64(item) IN (2, 99)",
        lambda n: n % 3 < 2,
    ),
}


@pytest.mark.parametrize(
    "engine,case,enabled",
    [
        (engine, case, 1)
        for engine in ("ordinary", "replicated")
        for case in ("scalar", "uint128", "tuple")
    ]
    + [("ordinary", "scalar", 0), ("ordinary", "computed", 1), ("ordinary", "barrier", 1)],
)
def test_typed_row_mask_union(engine, case, enabled):
    # This existing instance has unlimited background Set and temporary-data limits.
    table = f"typed_union_{engine}_{case}_{enabled}"
    engine_sql = "AggregatingMergeTree"
    if engine == "replicated":
        engine_sql = (
            "ReplicatedAggregatingMergeTree("
            f"'/clickhouse/tables/test_mutation_row_mask_set_limit/{table}', 'r1')"
        )
    first, second, deleted = TYPED_UNION_CASES["scalar" if case == "barrier" else case]
    commands = [
        f"UPDATE _row_exists = 0 WHERE {first}",
        f"UPDATE _row_exists = 0 WHERE {second}",
    ]
    if case == "barrier":
        commands = [
            "UPDATE _row_exists = 0 WHERE tenant IN (-1, -2)",
            "UPDATE _row_exists = 0 WHERE tenant IN (-1, -3)",
            "UPDATE spare = spare + 1 WHERE tenant = 0",
            "UPDATE _row_exists = 0 WHERE tenant IN (1, 2)",
            "UPDATE _row_exists = 0 WHERE tenant IN (1, 3)",
        ]
    try:
        replicated_node.query(
            f"CREATE TABLE {table} (tenant Int32, key_gap UInt8, item UInt128, "
            "s AggregateFunction(sum, Int64), spare SimpleAggregateFunction(anyLast, UInt8)) "
            f"ENGINE = {engine_sql} ORDER BY (tenant, key_gap, item) "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
            "min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, "
            "enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0, "
            f"enable_row_mask_typed_set_union = {enabled}"
        )
        replicated_node.query(
            f"INSERT INTO {table} SELECT toInt32(intDiv(number, 6)) - 1 AS seed_tenant, "
            "toUInt8(intDiv(number % 6, 3)) AS seed_gap, "
            "toUInt128('18446744073709551616') + toUInt128(number % 3 + 1) AS seed_item, "
            "sumState(toInt64(number + 1)), toUInt8(0) "
            "FROM numbers(18) GROUP BY seed_tenant, seed_gap, seed_item"
        )
        # Preserve an already cleared mask outside the new literal Sets.
        replicated_node.query(
            f"ALTER TABLE {table} UPDATE _row_exists = 0 WHERE tenant = 0 AND key_gap = 0 "
            "AND item = toUInt128('18446744073709551619') SETTINGS mutations_sync = 2"
        )
        start = replicated_node.query("SELECT toUnixTimestamp64Micro(now64(6))").strip()
        replicated_node.query(
            f"ALTER TABLE {table} " + ", ".join(commands) + " SETTINGS mutations_sync = 2"
        )
        dispatch = enabled and case not in ("computed", "barrier")
        assert_typed_union_part_log(
            typed_union_part_log(table, start),
            "count() = 1 AND countIf(error != 0) = 0 "
            f"AND sum(ProfileEvents['RowMaskTypedSetUnionBatches']) = {int(dispatch)}",
        )
        physical = []
        visible = []
        for n in range(18):
            key = [n // 6 - 1, n % 6 // 3, 2**64 + n % 3 + 1]
            masked = n == 8 or deleted(n)
            spare = int(case == "barrier" and n // 6 == 1)
            physical.append(
                "\t".join(map(str, key + [int(not masked), n + 1, spare, 1])) + "\n"
            )
            if not masked:
                visible.append("\t".join(map(str, key + [n + 1, spare])) + "\n")
        assert replicated_node.query(
            f"SELECT tenant, key_gap, item, _row_exists, sumMerge(s), spare, count() FROM {table} "
            "GROUP BY tenant, key_gap, item, _row_exists, spare ORDER BY tenant, key_gap, item "
            "SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0"
        ) == "".join(physical)
        assert replicated_node.query(
            f"SELECT tenant, key_gap, item, sumMerge(s), spare FROM {table} "
            "GROUP BY tenant, key_gap, item, spare ORDER BY tenant, key_gap, item "
            "SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0"
        ) == "".join(visible)
    finally:
        replicated_node.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.mark.parametrize(
    "case,enabled", [("scalar", 0), ("scalar", 1), ("tuple", 1), ("no_hit", 1)]
)
def test_typed_row_mask_union_precheck(case, enabled):
    table = f"typed_precheck_{case}_{enabled}"
    first = "row_key IN (toUInt64(2), toUInt64(4), toUInt64(6), toUInt64(6))"
    second = "row_key IN (toUInt64(6), toUInt64(8))"
    deleted = {1, 2, 3, 4, 63}
    if case == "tuple":
        first = (
            "(tenant, row_key) IN ((toInt32(0), toUInt64(2)), (toInt32(1), toUInt64(4)), "
            "(toInt32(-1), toUInt64(6)), (toInt32(-1), toUInt64(6)))"
        )
        second = "(tenant, row_key) IN ((toInt32(-1), toUInt64(6)), (toInt32(0), toUInt64(8)))"
    elif case == "no_hit":
        first = "row_key IN (toUInt64(1), toUInt64(3))"
        second = "row_key IN (toUInt64(3), toUInt64(5))"
        deleted = {1, 63}
    merges_stopped = False
    try:
        replicated_node.query(
            f"CREATE TABLE {table} (tenant Int32, row_key UInt64, row_state AggregateFunction(sum, Int64)) "
            "ENGINE = AggregatingMergeTree ORDER BY (row_key, tenant) "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
            "min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, "
            "enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0, "
            f"enable_row_mask_typed_set_union = {enabled}"
        )
        replicated_node.query(
            f"INSERT INTO {table} SELECT toInt32(toInt64(number) % 3 - 1) AS seed_tenant, "
            "toUInt64(number * 2) AS seed_key, sumState(toInt64(number + 1)) "
            "FROM numbers(64) GROUP BY seed_tenant, seed_key"
        )
        # An already masked row is inside the overlapping union; another is outside.
        replicated_node.query(
            f"ALTER TABLE {table} UPDATE _row_exists = 0 WHERE row_key IN (toUInt64(2), toUInt64(126)) "
            "SETTINGS mutations_sync = 2"
        )
        original_count = replicated_node.query(
            f"SELECT count() FROM {table} WHERE ({first}) OR ({second}) "
            "SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0"
        ).strip()
        assert original_count == ("0" if case == "no_hit" else "3")
        start = replicated_node.query("SELECT toUnixTimestamp64Micro(now64(6))").strip()
        replicated_node.query(f"SYSTEM STOP MERGES {table}")
        merges_stopped = True
        for predicate in (first, second):
            replicated_node.query(
                f"ALTER TABLE {table} UPDATE _row_exists = 0 WHERE {predicate} SETTINGS mutations_sync = 0"
            )
        mutation_ids = replicated_node.query(
            "SELECT arraySort(groupArray(mutation_id)) FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{table}' AND NOT is_done"
        ).strip()
        assert replicated_node.query(f"SELECT length({mutation_ids})") == "2\n"
        replicated_node.query(f"SYSTEM START MERGES {table}")
        merges_stopped = False
        # Compare the flattened IDs without deduplication: both original mutations occur once.
        source = typed_union_part_log(table, start) + f" AND hasAny(mutation_ids, {mutation_ids})"
        events = "AND sum(ProfileEvents['RowMaskTypedSetUnionBatches']) = 0"
        if case != "no_hit":
            events = (
                f"AND sum(ProfileEvents['RowMaskTypedSetUnionPrechecks']) = {enabled} "
                f"AND sum(ProfileEvents['RowMaskTypedSetUnionBatches']) = {enabled}"
            )
        assert_typed_union_part_log(
            source,
            "count() = 1 AND countIf(error != 0) = 0 "
            f"AND arraySort(arrayFlatten(groupArray(mutation_ids))) = {mutation_ids} "
            f"AND sum(ProfileEvents['MutationAffectedRowsUpperBound']) = {original_count} {events}",
        )
        physical = [
            "\t".join(map(str, [n % 3 - 1, n * 2, int(n not in deleted), n + 1, 1])) + "\n"
            for n in range(64)
        ]
        visible = [
            "\t".join(map(str, [n % 3 - 1, n * 2, n + 1])) + "\n"
            for n in range(64)
            if n not in deleted
        ]
        assert replicated_node.query(
            f"SELECT tenant, row_key, _row_exists, sumMerge(row_state), count() FROM {table} "
            "GROUP BY tenant, row_key, _row_exists ORDER BY row_key "
            "SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0"
        ) == "".join(physical)
        assert replicated_node.query(
            f"SELECT tenant, row_key, sumMerge(row_state) FROM {table} "
            "GROUP BY tenant, row_key ORDER BY row_key "
            "SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0"
        ) == "".join(visible)
    finally:
        try:
            if merges_stopped:
                replicated_node.query(f"SYSTEM START MERGES {table}")
        finally:
            replicated_node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_typed_row_mask_union_respects_background_set_limit():
    table = "typed_union_background_set_limit"
    try:
        node.query(
            f"CREATE TABLE {table} (n UInt64, payload AggregateFunction(sum, UInt64)) "
            "ENGINE = AggregatingMergeTree ORDER BY n "
            "SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, "
            "min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, "
            "enable_block_number_column = 0, enable_block_offset_column = 0, "
            "enable_row_mask_update_coalescing = 0, enable_row_mask_action_batching = 0, "
            "enable_row_mask_typed_set_union = 1"
        )
        node.query(
            f"INSERT INTO {table} SELECT toUInt64(number + 1) AS seed_n, "
            "sumState(toUInt64((number + 1) * 11)) FROM numbers(3) GROUP BY seed_n"
        )
        assert node.query(
            "SELECT part_type, part_storage_type FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active"
        ) == "Wide\tFull\n"
        start = node.query("SELECT toUnixTimestamp64Micro(now64(6))").strip()
        # Each integer Set fits the background limit of one; their union does not.
        node.query(
            f"ALTER TABLE {table} "
            "UPDATE _row_exists = 0 WHERE n IN (toUInt64(1)), "
            "UPDATE _row_exists = 0 WHERE n IN (toUInt64(2)) "
            "SETTINGS mutations_sync = 2"
        )
        assert node.query(
            f"SELECT n, _row_exists, sumMerge(payload), count() FROM {table} "
            "GROUP BY n, _row_exists ORDER BY n "
            "SETTINGS apply_deleted_mask = 0, apply_mutations_on_fly = 0"
        ) == "1\t0\t11\t1\n2\t0\t22\t1\n3\t1\t33\t1\n"
        assert node.query(
            f"SELECT n, sumMerge(payload), count() FROM {table} GROUP BY n ORDER BY n "
            "SETTINGS apply_deleted_mask = 1, apply_mutations_on_fly = 0"
        ) == "3\t33\t1\n"
        assert_eq_with_retry(
            node,
            "SYSTEM FLUSH LOGS part_log; SELECT count() = 1 AND countIf(error != 0) = 0 "
            "AND sum(ProfileEvents['RowMaskTypedSetUnionBatches']) = 0 "
            + typed_union_part_log(table, start)
            + " SETTINGS max_execution_time = 2",
            "1",
            retry_count=20,
            timeout=3,
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
