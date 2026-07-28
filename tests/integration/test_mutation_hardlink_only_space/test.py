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
    tmpfs=["/mutation_hardlink_only_small:size=100M"],
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


def fill(table, engine):
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
        SETTINGS storage_policy = 'small_only',
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
        SELECT number, concat('tok', toString(number % 1000)), randomString(180) FROM numbers(300000)
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
    assert "1" in node.query(f"CHECK TABLE {table}")


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
