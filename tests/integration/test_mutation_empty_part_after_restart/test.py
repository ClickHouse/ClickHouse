import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

WEDGE_REASON = "Unknown expression identifier"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def state(table):
    parts = node.query(
        "SELECT name, rows, part_type, active FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' ORDER BY name"
    )
    mutations = node.query(
        "SELECT mutation_id, is_done, latest_fail_reason, parts_postpone_reasons "
        f"FROM system.mutations WHERE database = currentDatabase() AND table = '{table}' "
        "ORDER BY mutation_id"
    )
    return f"{table} parts:\n{parts}{table} mutations:\n{mutations}"


def arm(table):
    """Leave an empty part on disk that owes a mutation reading a column the table no longer has.

    `DETACH PART` replaces the part with an empty one covering the same block range, so that empty
    part keeps the column list from before the rename. `old_parts_lifetime` keeps it on disk once
    cleanup outdates it, and dropping the rename entry leaves nothing to map its `b` to `d`.

    `sleep_before_loading_outdated_parts_ms` holds the post-restart window open: empty parts are not
    removed until outdated parts have loaded, so without it the removal can win the race against
    mutation scheduling and the scenario would not arm at all.
    """
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (a String, b String, c String MATERIALIZED concat(a, '!'))
        ENGINE = MergeTree ORDER BY a
        SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
                 old_parts_lifetime = 10000, sleep_before_loading_outdated_parts_ms = 30000
        """
    )
    node.query(f"INSERT INTO {table} VALUES ('x', 'y')")
    node.query(f"ALTER TABLE {table} DETACH PART 'all_1_1_0'")
    node.query(f"ALTER TABLE {table} MODIFY COLUMN b Nullable(String)")
    node.query(f"ALTER TABLE {table} ATTACH PART 'all_1_1_0'")
    node.query(f"ALTER TABLE {table} RENAME COLUMN b TO d SETTINGS mutations_sync = 1")
    node.query(f"ALTER TABLE {table} ADD PROJECTION p_ad (SELECT a, d ORDER BY a)")
    node.query(
        f"ALTER TABLE {table} MATERIALIZE COLUMN c, MATERIALIZE PROJECTION p_ad "
        "SETTINGS mutations_sync = 1"
    )

    rename_mutation = node.query(
        "SELECT mutation_id FROM system.mutations "
        f"WHERE database = currentDatabase() AND table = '{table}' AND command ILIKE '%RENAME COLUMN%'"
    ).strip()
    assert rename_mutation, f"no rename mutation to drop\n{state(table)}"
    node.query(
        f"KILL MUTATION WHERE database = currentDatabase() AND table = '{table}' "
        f"AND mutation_id = '{rename_mutation}' SYNC"
    )

    empty_part = node.query(
        "SELECT rows, part_type, active FROM system.parts "
        f"WHERE database = currentDatabase() AND table = '{table}' AND name = 'all_1_1_1'"
    ).strip()
    # A Compact part would exercise a different read path, so the scenario is only armed when the
    # empty part is Wide and still on disk.
    assert empty_part == "0\tWide\t0", f"empty part not armed: {empty_part!r}\n{state(table)}"
    pending = node.query(
        "SELECT count() FROM system.mutations "
        f"WHERE database = currentDatabase() AND table = '{table}' AND command ILIKE '%MODIFY COLUMN%'"
    ).strip()
    assert pending == "1", f"the replayed mutation is gone\n{state(table)}"


def poll(query, expected, timeout=120):
    deadline = time.monotonic() + timeout
    result = None
    while time.monotonic() < deadline:
        result = node.query(query).strip()
        if result == expected:
            return True
        time.sleep(1)
    print(f"poll timed out, last result {result!r}, wanted {expected!r}")
    return False


def test_mutation_of_empty_part_after_restart(started_cluster):
    arm("t_wedge")

    # With empty-part removal disabled nothing will drop the part, so the mutation has to be
    # attempted and the pre-existing behaviour must be kept.
    arm("t_wedge_kept")
    node.query("ALTER TABLE t_wedge_kept MODIFY SETTING remove_empty_parts = 0")

    node.restart_clickhouse()

    # The restart revives the empty part as Active while mutation scheduling is already running.
    # Asserting that here, inside the window the pinned sleep holds open, keeps a run where the
    # part was never revived from passing without having tested anything.
    assert poll(
        "SELECT active FROM system.parts WHERE database = currentDatabase() "
        "AND table = 't_wedge' AND name = 'all_1_1_1'",
        "1",
        timeout=10,
    ), f"the empty part was not revived as active by the restart\n{state('t_wedge')}"

    # Still inside that window: the mutation must be reported as postponed for this part, which is
    # what a user reads to tell a skipped empty part from a stalled mutation.
    assert poll(
        "SELECT parts_postpone_reasons['all_1_1_1'] FROM system.mutations "
        "WHERE database = currentDatabase() AND table = 't_wedge' AND command ILIKE '%MODIFY COLUMN%'",
        "Empty part will be dropped instead of mutated",
        timeout=15,
    ), f"the skip did not record a postpone reason\n{state('t_wedge')}"

    assert poll(
        "SELECT countIf(is_done = 0) = 0 AND countIf(latest_fail_reason != '') = 0 "
        "FROM system.mutations WHERE database = currentDatabase() AND table = 't_wedge'",
        "1",
    ), f"the mutation never completed\n{state('t_wedge')}"
    # The mutation completes by the part being dropped, not by being mutated, so no mutated
    # descendant of it may exist. `old_parts_lifetime` keeps the dropped part itself listed.
    assert poll(
        "SELECT countIf(active) = 0 AND countIf(name LIKE 'all\\_1\\_1\\_1\\_%') = 0 "
        "FROM system.parts WHERE database = currentDatabase() AND table = 't_wedge' "
        "AND name LIKE 'all\\_1\\_1\\_1%'",
        "1",
    ), f"the empty part was not dropped\n{state('t_wedge')}"

    assert node.query("SELECT a, d, c FROM t_wedge ORDER BY a") == "x\ty\tx!\n"
    assert (
        node.query(
            "SELECT type FROM system.parts_columns WHERE database = currentDatabase() "
            "AND table = 't_wedge' AND active AND column = 'd'"
        )
        == "Nullable(String)\n"
    )

    assert poll(
        "SELECT latest_fail_reason ILIKE '%" + WEDGE_REASON + "%' FROM system.mutations "
        "WHERE database = currentDatabase() AND table = 't_wedge_kept' AND command ILIKE '%MODIFY COLUMN%'",
        "1",
    ), f"remove_empty_parts = 0 no longer attempts the mutation\n{state('t_wedge_kept')}"
    assert (
        node.query(
            "SELECT is_done FROM system.mutations WHERE database = currentDatabase() "
            "AND table = 't_wedge_kept' AND command ILIKE '%MODIFY COLUMN%'"
        ).strip()
        == "0"
    ), state("t_wedge_kept")

    node.query("DROP TABLE t_wedge SYNC")
    node.query("DROP TABLE t_wedge_kept SYNC")


def test_mutation_of_empty_part_with_cleanup_stopped(started_cluster):
    table = "t_stop_cleanup"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a")
    # Stopped before the empty part is made, so that nothing can remove it in between.
    node.query(f"SYSTEM STOP CLEANUP {table}")
    try:
        node.query(f"INSERT INTO {table} VALUES (1)")
        node.query(f"ALTER TABLE {table} DELETE WHERE 1 SETTINGS mutations_sync = 1")

        empty_parts = node.query(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active AND rows = 0"
        ).strip()
        assert empty_parts == "1", f"no empty part to mutate\n{state(table)}"

        # Not `mutations_sync = 1`: while cleanup is stopped nothing will remove this part, so a
        # mutation that is skipped instead of run never finishes and the query would never return.
        node.query(f"ALTER TABLE {table} DELETE WHERE a = 1")
        assert poll(
            "SELECT countIf(is_done = 0) = 0 FROM system.mutations "
            f"WHERE database = currentDatabase() AND table = '{table}'",
            "1",
            timeout=60,
        ), f"the mutation was skipped while cleanup could not run\n{state(table)}"
    finally:
        node.query(f"SYSTEM START CLEANUP {table}")
        node.query(f"DROP TABLE {table} SYNC")


def test_mutation_of_empty_part_with_cleanup_interval_deferred(started_cluster):
    table = "t_long_interval"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    # The period the periodic part cleanups share is pushed out of the run, so the empty part can
    # only be removed by a request that does not wait for it.
    node.query(
        f"CREATE TABLE {table} (a UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS merge_tree_clear_old_parts_interval_seconds = 100000"
    )
    try:
        node.query(f"INSERT INTO {table} VALUES (1)")
        # The source part is not empty, so this mutation is run rather than skipped, and it leaves
        # the empty part behind.
        node.query(f"ALTER TABLE {table} DELETE WHERE 1 SETTINGS mutations_sync = 1")

        empty_parts = node.query(
            "SELECT count() FROM system.parts WHERE database = currentDatabase() "
            f"AND table = '{table}' AND active AND rows = 0"
        ).strip()
        assert empty_parts == "1", f"no empty part to mutate\n{state(table)}"

        # `max_execution_time` bounds the wait, so a mutation that is skipped without anything
        # dropping the part fails here in a minute instead of hanging for the pinned interval.
        node.query(
            f"ALTER TABLE {table} DELETE WHERE a = 1 "
            "SETTINGS mutations_sync = 1, max_execution_time = 60"
        )
        assert (
            node.query(
                "SELECT countIf(is_done = 0) FROM system.mutations "
                f"WHERE database = currentDatabase() AND table = '{table}'"
            ).strip()
            == "0"
        ), state(table)
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
