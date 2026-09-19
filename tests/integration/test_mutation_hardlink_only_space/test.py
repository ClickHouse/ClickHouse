import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# A real, size-limited disk. The stateless test drives the same code through a failpoint that pins the
# free-space budget to 1 byte, which shows that the selection guard lets the mutation through but says
# nothing about the reservation the mutation then has to take. Only a genuinely small disk can show
# that: here the budget is real, so a mutation whose reservation still asked for the whole source part
# would fail to reserve and be postponed exactly as on master.
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_configuration.xml"],
    with_zookeeper=True,
    stay_alive=True,
    tmpfs=[
        "/mutation_hardlink_only_small:size=100M",
        # Roomy on purpose: the move-between-admission-and-execution case is about the reservation
        # naming the wrong DISK, not about space, so this one must not run out.
        "/mutation_hardlink_only_other:size=400M",
    ],
    macros={"replica": "r1"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def part_bytes(table):
    return int(
        node.query(
            f"SELECT sum(bytes_on_disk) FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
    )


def disk_free():
    return int(
        node.query(
            "SELECT unreserved_space FROM system.disks WHERE name = 'small_disk'"
        ).strip()
    )


def fill(table, engine, policy="small_only", rows=300000):
    # The whole point of the fixture is that the disk is nearly full, so nothing from an earlier case
    # may still be occupying it.
    for other in node.query(
        "SELECT name FROM system.tables WHERE database = 'default'"
    ).split():
        node.query(f"DROP TABLE IF EXISTS {other} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (id UInt64, s String, payload String,
            INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
        ENGINE = {engine} ORDER BY id
        SETTINGS storage_policy = '{policy}',
                 min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
                 packed_skip_index_max_bytes = 0
        """
    )
    # One incompressible part taking a bit over half the disk, in a single block so that no merge is
    # needed: the part must be well above the free-space budget that is left, while that budget must
    # still be well above the small reservation the fix asks for. Both are asserted below.
    node.query(
        f"""
        INSERT INTO {table}
        SELECT number, concat('tok', toString(number % 1000)), randomString(180) FROM numbers({rows})
        """,
        settings={"max_insert_block_size": 1000000, "min_insert_block_size_rows": 1000000},
    )


def assert_fixture_is_meaningful(table):
    """Both inequalities the case rests on, asserted rather than assumed.

    part_size > free / 1.1        so the selection guard defers the mutation on master
                                  (CompactionStatistics::getMaxSourcePartBytesForMutation divides the
                                  maximum unreserved free space by DISK_USAGE_COEFFICIENT_TO_RESERVE)
    1 MB      < free / 1.1        so the small reservation the fix takes can actually be granted
                                  (MergeTreeData::tryReserveSpace clamps every request up to
                                  RESERVATION_MIN_ESTIMATION_SIZE = 1 MB)
    """
    size = part_bytes(table)
    budget = disk_free() / 1.1
    assert size > budget, f"part {size} must exceed the budget {budget}"
    assert 1024 * 1024 < budget, f"1 MB must fit in the budget {budget}"


def assert_dropped_and_partial(table, mutation_count_before):
    assert (
        int(node.query(f"SELECT count() FROM system.data_skipping_indices WHERE table = '{table}'"))
        == 0
    )
    assert int(node.query(f"SELECT count() FROM {table}")) == 300000
    assert (
        int(
            node.query(
                f"SELECT count() FROM system.mutations WHERE table = '{table}' AND NOT is_done"
            )
        )
        == 0
    )
    assert (
        node.query(f"SELECT count() FROM system.mutations WHERE table = '{table}' AND notEmpty(latest_fail_reason)").strip()
        == "0"
    )
    # It must not have completed by rewriting the whole part: that needs the space it was deferred for.
    node.query("SYSTEM FLUSH LOGS part_log")
    routes = node.query(
        f"""
        SELECT sum(ProfileEvents['MutationSomePartColumns']), sum(ProfileEvents['MutationAllPartColumns'])
        FROM system.part_log WHERE table = '{table}' AND event_type = 'MutatePart'
        """
    ).split()
    assert int(routes[0]) > mutation_count_before, "the mutation must take the partial route"
    assert int(routes[1]) == 0, "the mutation must not rewrite the whole part"
    assert node.query(f"CHECK TABLE {table} SETTINGS check_query_single_value_result = 1") == "1\n"


def test_drop_index_on_a_nearly_full_disk(start_cluster):
    fill("mt_small", "MergeTree")
    assert_fixture_is_meaningful("mt_small")

    node.query(
        "ALTER TABLE mt_small DROP INDEX idx_s",
        settings={"alter_sync": 2, "mutations_sync": 2},
    )
    assert_dropped_and_partial("mt_small", 0)


def test_delete_on_a_nearly_full_disk_is_still_deferred(start_cluster):
    fill("mt_small_delete", "MergeTree")
    assert_fixture_is_meaningful("mt_small_delete")

    node.query("ALTER TABLE mt_small_delete DELETE WHERE id = 1", settings={"alter_sync": 0})
    # A DELETE rewrites the part, so it must stay deferred rather than fill the disk.
    assert node.query_with_retry(
        """
        SELECT arrayExists(reason -> reason = 'Exceed max source part size',
                           mapValues(parts_postpone_reasons))
        FROM system.mutations WHERE table = 'mt_small_delete' AND NOT is_done
        """,
        check_callback=lambda r: r.strip() == "1",
    ).strip() == "1"
    assert (
        node.query(
            "SELECT count() FROM system.mutations WHERE table = 'mt_small_delete' AND notEmpty(latest_fail_reason)"
        ).strip()
        == "0"
    )


def test_drop_index_on_a_nearly_full_disk_replicated(start_cluster):
    fill(
        "rmt_small",
        "ReplicatedMergeTree('/clickhouse/tables/rmt_small', '{replica}')",
    )
    assert_fixture_is_meaningful("rmt_small")

    node.query(
        "ALTER TABLE rmt_small DROP INDEX idx_s",
        settings={"alter_sync": 2, "mutations_sync": 2},
    )
    assert_dropped_and_partial("rmt_small", 0)


def test_delete_on_a_nearly_full_disk_replicated_is_still_deferred(start_cluster):
    fill(
        "rmt_small_delete",
        "ReplicatedMergeTree('/clickhouse/tables/rmt_small_delete', '{replica}')",
    )
    assert_fixture_is_meaningful("rmt_small_delete")

    node.query("ALTER TABLE rmt_small_delete DELETE WHERE id = 1", settings={"alter_sync": 0})
    # A DELETE rewrites the part, so it must stay deferred rather than fill the disk.
    assert node.query_with_retry(
        """
        SELECT arrayExists(reason -> reason = 'Exceed max source part size',
                           mapValues(parts_postpone_reasons))
        FROM system.mutations WHERE table = 'rmt_small_delete' AND NOT is_done
        """,
        check_callback=lambda r: r.strip() == "1",
    ).strip() == "1"
    assert (
        node.query(
            "SELECT count() FROM system.mutations WHERE table = 'rmt_small_delete' AND notEmpty(latest_fail_reason)"
        ).strip()
        == "0"
    )


def test_source_part_moved_between_admission_and_execution(start_cluster):
    """The reservation must name the disk the source part is on WHEN THE MUTATION RUNS.

    A replicated mutation admitted as hardlink-only reserves its small amount of space at selection
    time, on the disk of the part selection saw. Execution resolves the active source part again, and
    the two can disagree: a move that started BEFORE the MUTATE_PART entry existed passed its own
    `can_move` check then, and MergeTreePartsMover::swapClonedPart only re-checks that an active part
    of that name still exists - its own comment says "we don't block moving parts for merges or
    mutations". Since the result part's path comes from that reservation and a hardlink cannot cross
    disks, the reservation has to be re-taken on the part's current disk.

    Ordering matters and is the whole difficulty: once the entry exists, BOTH move paths refuse the
    part (`MergeTreeData::checkPartsForMove` and the background mover's `can_move` both reject a part
    with `partIsAssignedToBackgroundOperation`, which for a replicated table is `queue.isVirtualPart`).
    So the move is started first and parked mid-flight - after it cloned onto the other disk, before it
    swaps - with `stop_moving_part_before_swap_with_active`; only then is the mutation issued.
    """
    fill(
        "rmt_moved",
        "ReplicatedMergeTree('/clickhouse/tables/rmt_moved', '{replica}')",
        policy="two_disks",
        # Small: this case is about the disk the reservation names, not about space, and the part has
        # to be cheap to clone while the move is parked.
        rows=20000,
    )
    part = node.query(
        "SELECT name FROM system.parts WHERE table = 'rmt_moved' AND active"
    ).strip()
    disk_before = node.query(
        "SELECT disk_name FROM system.parts WHERE table = 'rmt_moved' AND active"
    ).strip()
    other = "other_disk" if disk_before == "small_disk" else "small_disk"

    # 1. Park a move after the clone, before the swap. The part is still active on its old disk here,
    #    so the mutation that follows is admitted against THAT disk.
    node.query("SYSTEM ENABLE FAILPOINT stop_moving_part_before_swap_with_active")
    move = node.get_query_request(
        f"ALTER TABLE rmt_moved MOVE PART '{part}' TO DISK '{other}'"
    )
    node.query("SYSTEM WAIT FAILPOINT stop_moving_part_before_swap_with_active PAUSE")

    try:
        # 2. Now admit the mutation. Selection reserves on the part's CURRENT (old) disk.
        node.query("SYSTEM ENABLE FAILPOINT rmt_mutate_task_pause_in_prepare")
        node.query(
            "ALTER TABLE rmt_moved DROP INDEX idx_s", settings={"alter_sync": 0}
        )
        node.query("SYSTEM WAIT FAILPOINT rmt_mutate_task_pause_in_prepare PAUSE")

        # 3. Let the parked move finish its swap while the mutation waits. The active part is now on
        #    the other disk, while the mutation still carries a reservation naming the old one.
        #    NOTIFY before DISABLE: disabling first destroys the wait channel the move sits on.
        node.query("SYSTEM NOTIFY FAILPOINT stop_moving_part_before_swap_with_active")
        node.query("SYSTEM DISABLE FAILPOINT stop_moving_part_before_swap_with_active")
        move.get_answer()
        assert (
            node.query(
                "SELECT disk_name FROM system.parts WHERE table = 'rmt_moved' AND active"
            ).strip()
            == other
        ), "the move must have completed while the mutation was paused"

        # 4. Release the mutation into exactly the disagreement this case exists for.
        node.query("SYSTEM NOTIFY FAILPOINT rmt_mutate_task_pause_in_prepare")
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_in_prepare")
        # Already disabled on the happy path; harmless to repeat, and required if step 2 or 3 threw.
        node.query("SYSTEM DISABLE FAILPOINT stop_moving_part_before_swap_with_active")

    # The mutation must complete on the part's CURRENT disk rather than fail on a cross-disk hardlink.
    assert (
        node.query_with_retry(
            "SELECT count() FROM system.mutations WHERE table = 'rmt_moved' AND NOT is_done",
            check_callback=lambda r: r.strip() == "0",
        ).strip()
        == "0"
    )
    assert (
        int(
            node.query(
                "SELECT count() FROM system.data_skipping_indices WHERE table = 'rmt_moved'"
            )
        )
        == 0
    )
    assert int(node.query("SELECT count() FROM rmt_moved")) == 20000
    assert (
        node.query(
            "SELECT count() FROM system.mutations WHERE table = 'rmt_moved' AND notEmpty(latest_fail_reason)"
        ).strip()
        == "0"
    )
    # The result part lives where the source part ended up, which is what a hardlink requires.
    assert (
        node.query(
            "SELECT disk_name FROM system.parts WHERE table = 'rmt_moved' AND active"
        ).strip()
        == other
    )
    assert node.query("CHECK TABLE rmt_moved SETTINGS check_query_single_value_result = 1") == "1\n"

    # The load-bearing assertion. Without the re-validation the entry hardlinks from the disk the
    # reservation named, which the part has left, and the attempt fails with CANNOT_LINK / ENOENT. That
    # failure is RECOVERABLE - the next selection pass reserves on the part's current disk and the
    # mutation then completes - so every assertion above still holds without the fix and only the
    # error itself distinguishes the two. It must never have been attempted even once.
    assert not node.contains_in_log(
        "Cannot link", filename="clickhouse-server.err.log"
    ), "a cross-disk hardlink must never be attempted"
