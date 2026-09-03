# Mutations run their "is the part touched" check query with the settings of the
# global context, so `force_data_skipping_indices` can only be exercised for them
# through the default profile of the server. The index-analysis shortcut and the
# empty-part shortcut in `isStorageTouchedByMutations` must both be bypassed when
# the setting is set, otherwise a mutation whose predicate does not use the forced
# index would silently pass instead of failing with `INDEX_NOT_USED`.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_forced = cluster.add_instance(
    "node_forced", user_configs=["configs/users.d/force_indices.xml"]
)
node = cluster.add_instance("node")

INDEX_NOT_USED_MESSAGE = (
    "Index `idx` is not used and setting 'force_data_skipping_indices' contains it"
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_table(instance, table):
    instance.query(f"DROP TABLE IF EXISTS {table} SYNC")
    # One part per partition, so background merges cannot change the number of parts
    # a mutation is checked against.
    instance.query(f"""
        CREATE TABLE {table} (p UInt8, id UInt64, v UInt64, INDEX idx v TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree PARTITION BY p ORDER BY id
        SETTINGS remove_empty_parts = 0
        """)


def drop_table(instance, table):
    instance.query(
        f"KILL MUTATION WHERE database = 'default' AND table = '{table}' SYNC"
    )
    instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


def insert_two_parts(instance, table):
    instance.query(f"INSERT INTO {table} SELECT 0, number, 1 FROM numbers(100)")
    instance.query(f"INSERT INTO {table} SELECT 1, 100 + number, 0 FROM numbers(100)")


def untouched_parts_by_index_analysis(instance):
    return int(
        instance.query(
            "SELECT sum(value) FROM system.events WHERE event = 'MutationUntouchedPartsByIndexAnalysis'"
        )
    )


def mutate(instance, query):
    instance.query(f"{query} SETTINGS mutations_sync = 2")


def mutate_and_expect_index_not_used(instance, table, query):
    error = instance.query_and_get_error(f"{query} SETTINGS mutations_sync = 2")
    assert INDEX_NOT_USED_MESSAGE in error
    assert (
        instance.query(
            f"SELECT latest_fail_error_code_name FROM system.mutations WHERE database = 'default' AND table = '{table}' AND NOT is_done"
        ).strip()
        == "INDEX_NOT_USED"
    )
    instance.query(
        f"KILL MUTATION WHERE database = 'default' AND table = '{table}' SYNC"
    )
    return error


def make_single_empty_part(instance, table):
    instance.query(f"INSERT INTO {table} SELECT 0, number, 1 FROM numbers(100)")
    # The predicate uses `idx`, so the mutation passes the forced-index check on both nodes.
    mutate(instance, f"ALTER TABLE {table} DELETE WHERE v = 1")
    parts = instance.query(
        f"SELECT name, rows FROM system.parts WHERE database = 'default' AND table = '{table}' AND active"
    ).split()
    assert parts[1] == "0", parts
    return parts[0]


def test_forced_index_rejects_part_provable_by_index_analysis():
    table = "t_forced"
    create_table(node_forced, table)
    try:
        insert_two_parts(node_forced, table)
        events_before = untouched_parts_by_index_analysis(node_forced)

        # The primary key proves both parts untouched, but the shortcut must not
        # bypass the check query, which rejects a predicate not using `idx`.
        mutate_and_expect_index_not_used(
            node_forced, table, f"ALTER TABLE {table} UPDATE v = 7 WHERE id = 1000"
        )
        mutate_and_expect_index_not_used(
            node_forced, table, f"ALTER TABLE {table} DELETE WHERE id = 1000"
        )

        # A predicate using `idx` still passes the check and touches only the matching part.
        mutate(node_forced, f"ALTER TABLE {table} UPDATE v = 7 WHERE v = 1")
        assert node_forced.query(
            f"SELECT sum(v), count() FROM {table} SETTINGS use_skip_indexes = 0"
        ).split() == ["700", "200"]

        assert untouched_parts_by_index_analysis(node_forced) == events_before
    finally:
        drop_table(node_forced, table)


def test_forced_index_rejects_empty_part():
    table = "t_forced_empty"
    create_table(node_forced, table)
    try:
        empty_part = make_single_empty_part(node_forced, table)
        events_before = untouched_parts_by_index_analysis(node_forced)

        # An empty part has trivially no rows to touch, yet the check query must
        # still run and reject a predicate not using `idx`.
        error = mutate_and_expect_index_not_used(
            node_forced, table, f"ALTER TABLE {table} DELETE WHERE id = 5"
        )
        assert f"with part '{empty_part}'" in error

        assert untouched_parts_by_index_analysis(node_forced) == events_before
    finally:
        drop_table(node_forced, table)


def test_untouched_parts_without_forced_index():
    table = "t_control"
    create_table(node, table)
    try:
        insert_two_parts(node, table)
        events_before = untouched_parts_by_index_analysis(node)

        # Without the forced index both parts are proven untouched by the primary key.
        mutate(node, f"ALTER TABLE {table} UPDATE v = 7 WHERE id = 1000")
        assert untouched_parts_by_index_analysis(node) == events_before + 2
        assert node.query(f"SELECT sum(v), count() FROM {table}").split() == [
            "100",
            "200",
        ]
    finally:
        drop_table(node, table)


def test_empty_part_without_forced_index():
    table = "t_control_empty"
    create_table(node, table)
    try:
        make_single_empty_part(node, table)
        events_before = untouched_parts_by_index_analysis(node)

        # The empty part is skipped before index analysis, so it is not counted as
        # proven untouched by it.
        mutate(node, f"ALTER TABLE {table} DELETE WHERE id = 5")
        assert untouched_parts_by_index_analysis(node) == events_before
    finally:
        drop_table(node, table)
