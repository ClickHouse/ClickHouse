"""Regression test for the replicated merge memory reservation path.

`MergeFromLogEntryTask::prepare` reserves memory for the merge's input/output IO buffers up front
(unconditionally - this replica is already committed to running the log entry), sized against the
resolved destination disk. This drives a real ZooKeeper-backed merge through that path and observes
the reservation itself: the task is parked on the `rmt_merge_task_pause_after_reserve` failpoint right
after it has reserved, the `MergesMutationsMemoryReservation` metric is read while the merge is held
there, and after the failpoint is released the merge must complete and release the reservation.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)
# A separate node, so that the small `merges_mutations_memory_usage_soft_limit` the throttling test
# installs at runtime cannot interfere with the test above.
node_limit = cluster.add_instance(
    "node_limit",
    main_configs=["configs/merge_memory_soft_limit.xml"],
    with_zookeeper=True,
)

SOFT_LIMIT_CONFIG = "/etc/clickhouse-server/config.d/merge_memory_soft_limit.xml"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def reserved_memory():
    return int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'"
        ).strip()
    )


def active_parts():
    return int(
        node.query(
            "SELECT count() FROM system.parts"
            " WHERE database = 'default' AND table = 't_replicated_merge_reservation' AND active"
        ).strip()
    )


def test_replicated_merge_reserves_memory(started_cluster):
    node.query("""
        CREATE TABLE t_replicated_merge_reservation (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_replicated_merge_reservation', 'r1')
        ORDER BY k
        """)

    # Only this table's merges may run during the test, so the process-wide metric below can only
    # reflect the reservation of this one replicated merge.
    node.query("SYSTEM STOP MERGES")

    node.query(
        "INSERT INTO t_replicated_merge_reservation SELECT number, repeat('a', 100) FROM numbers(10000)"
    )
    node.query(
        "INSERT INTO t_replicated_merge_reservation"
        " SELECT number, repeat('b', 100) FROM numbers(10000, 10000)"
    )
    assert active_parts() == 2
    assert reserved_memory() == 0

    node.query("SYSTEM ENABLE FAILPOINT rmt_merge_task_pause_after_reserve")
    try:
        node.query("SYSTEM START MERGES t_replicated_merge_reservation")
        # The OPTIMIZE creates a MERGE_PARTS log entry and returns; the entry is executed in the
        # background by MergeFromLogEntryTask, which reserves and then parks on the failpoint.
        node.query(
            "OPTIMIZE TABLE t_replicated_merge_reservation", settings={"alter_sync": 0}
        )

        assert_eq_with_retry(
            node,
            "SELECT value > 0 FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'",
            "1",
        )

        # The reservation is a sustained floor while the task is held on the failpoint - not a
        # transient blip - and the merge has not executed (both source parts are still active).
        for _ in range(5):
            assert reserved_memory() > 0
            assert active_parts() == 2
            time.sleep(1)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_merge_task_pause_after_reserve")

    # Released from the failpoint, the merge runs to completion and releases its reservation.
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.parts"
        " WHERE database = 'default' AND table = 't_replicated_merge_reservation' AND active",
        "1",
    )
    assert_eq_with_retry(
        node,
        "SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'",
        "0",
    )
    assert (
        node.query("SELECT count() FROM t_replicated_merge_reservation").strip()
        == "20000"
    )


def reserved_memory_on(instance):
    return int(
        instance.query(
            "SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'"
        ).strip()
    )


def active_parts_of(instance, table):
    return int(
        instance.query(
            "SELECT count() FROM system.parts"
            f" WHERE database = 'default' AND table = '{table}' AND active"
        ).strip()
    )


def merges_rejected_by_memory_limit(instance):
    return int(
        instance.query(
            "SELECT sum(value) FROM system.events"
            " WHERE event = 'MergesRejectedByMemoryLimit'"
        ).strip()
        or 0
    )


def set_soft_limit(instance, limit):
    instance.replace_config(
        SOFT_LIMIT_CONFIG,
        "<clickhouse><merges_mutations_memory_usage_soft_limit>"
        f"{limit}"
        "</merges_mutations_memory_usage_soft_limit></clickhouse>",
    )
    instance.query("SYSTEM RELOAD CONFIG")


def create_and_fill(instance, table, extra_settings=""):
    instance.query(f"""
        CREATE TABLE {table} (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', 'r1')
        ORDER BY k
        SETTINGS merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 1000{extra_settings}
        """)
    instance.query(
        f"INSERT INTO {table} SELECT number, repeat('a', 100) FROM numbers(10000)"
    )
    instance.query(
        f"INSERT INTO {table} SELECT number, repeat('b', 100) FROM numbers(10000, 10000)"
    )
    assert active_parts_of(instance, table) == 2


def test_replicated_reservation_is_unconditional_and_throttles_further_merges(
    started_cluster,
):
    """The replicated path's two promises, each pinned by a regression that would break it.

    1. `MergeFromLogEntryTask::prepare` reserves unconditionally: a second replicated merge log
       entry reserves even though the soft limit is already exhausted by the first reservation.
       `MergeMemoryReservation::tryReserve` would refuse that second reservation, so the observed
       total staying at one reservation's worth is exactly the regression this catches.
    2. That reservation throttles the *selection* of further merges: while it is held, a third
       table's ordinary background merge is never selected (`canEnqueueBackgroundTask` refuses it,
       counted by `MergesRejectedByMemoryLimit`) and its parts are merged only after the held
       reservations are released.
    """

    instance = node_limit

    # Merges are enabled per table below, so the process-wide reservation metric can only reflect
    # the merges this test drives.
    instance.query("SYSTEM STOP MERGES")

    for table in ("t_first", "t_second"):
        create_and_fill(instance, table)
    # The third table's merge is the one that must go through selection, so make its selection
    # unconditional: whether it happens depends only on the memory gate, never on how attractive
    # the merge selector finds two small parts.
    create_and_fill(
        instance,
        "t_selected",
        extra_settings=", min_age_to_force_merge_seconds = 1",
    )
    assert reserved_memory_on(instance) == 0

    instance.query("SYSTEM ENABLE FAILPOINT rmt_merge_task_pause_after_reserve")
    try:
        # Phase 1: one merge log entry, executed while the limit is still disabled. Its reservation
        # is the unit of memory every assertion below is expressed in.
        instance.query("SYSTEM START MERGES t_first")
        instance.query("OPTIMIZE TABLE t_first", settings={"alter_sync": 0})
        assert_eq_with_retry(
            instance,
            "SELECT value > 0 FROM system.metrics"
            " WHERE metric = 'MergesMutationsMemoryReservation'",
            "1",
        )
        one_reservation = reserved_memory_on(instance)

        # Phase 2: with the limit at exactly one reservation, the gate is now closed - and the
        # second log entry must still reserve, because this replica is already committed to it.
        set_soft_limit(instance, one_reservation)
        instance.query("SYSTEM START MERGES t_second")
        instance.query("OPTIMIZE TABLE t_second", settings={"alter_sync": 0})
        assert_eq_with_retry(
            instance,
            "SELECT value >= "
            f"{one_reservation + one_reservation // 2}"
            " FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'",
            "1",
        )
        assert active_parts_of(instance, "t_first") == 2
        assert active_parts_of(instance, "t_second") == 2

        # Phase 3: an ordinary background merge (no OPTIMIZE - this one goes through selection)
        # cannot be selected while those reservations are held.
        rejected_before = merges_rejected_by_memory_limit(instance)
        instance.query("SYSTEM START MERGES t_selected")
        for _ in range(5):
            assert active_parts_of(instance, "t_selected") == 2
            time.sleep(1)
        assert merges_rejected_by_memory_limit(instance) > rejected_before
    finally:
        instance.query("SYSTEM DISABLE FAILPOINT rmt_merge_task_pause_after_reserve")

    # Released, the held merges complete, and only then is the third table's merge selected.
    for table in ("t_first", "t_second", "t_selected"):
        assert_eq_with_retry(
            instance,
            "SELECT count() FROM system.parts"
            f" WHERE database = 'default' AND table = '{table}' AND active",
            "1",
            retry_count=60,
            sleep_time=1,
        )
        assert instance.query(f"SELECT count() FROM {table}").strip() == "20000"
    assert_eq_with_retry(
        instance,
        "SELECT value FROM system.metrics"
        " WHERE metric = 'MergesMutationsMemoryReservation'",
        "0",
    )
