# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

# This test verifies that memory tracking does not have significant drift,
# in other words, every allocation should be taken into account at the global
# memory tracker.
#
# So we are running some queries with GROUP BY to make some allocations,
# and after we are checking MemoryTracking metric from system.metrics,
# and check that it does not changes too frequently.
#
# Also note, that correcting MemoryTracking from a measurement of the memory really
# used by the process is enabled by default and had been disabled for `node` in
# no_memory_tracker_correction.xml, and that syncing MemoryTracking with RSS from the
# asynchronous metrics had been disabled in asynchronous_metrics_update_period_s.xml.
#
# The `node_corrected` instance checks the opposite, default, configuration: there
# MemoryTracking follows the measured usage (it moves with retained and purged pages that
# never went through the tracker), while MemoryTrackingUncorrected keeps the plain counter
# of allocations.

import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/no_system_log.xml",
        "configs/asynchronous_metrics_update_period_s.xml",
        "configs/no_memory_tracker_correction.xml",
    ],
    user_configs=[
        "configs/users.d/overrides.xml",
    ],
)

node_corrected = cluster.add_instance(
    "node_corrected",
    main_configs=[
        "configs/no_system_log.xml",
        "configs/memory_tracker_correction.xml",
    ],
    user_configs=[
        "configs/users.d/overrides.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


query_settings = {
    "max_threads": 1,
    "log_queries": 0,
}
sample_query = "SELECT groupArray(repeat('a', 1000)) FROM numbers(10000) GROUP BY number%10 FORMAT JSON"


def query(*args, **kwargs):
    if "settings" not in kwargs:
        kwargs["settings"] = query_settings
    else:
        kwargs["settings"].update(query_settings)
    return node.query(*args, **kwargs)


def http_query(*args, **kwargs):
    if "params" not in kwargs:
        kwargs["params"] = query_settings
    else:
        kwargs["params"].update(query_settings)
    return node.http_query(*args, **kwargs)


def get_MemoryTracking():
    return int(
        http_query("SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'")
    )


def check_memory(memory):
    # bytes -> megabytes
    memory = [*map(lambda x: int(int(x) / 1024 / 1024), memory)]
    # 3 changes to MemoryTracking is minimum, since:
    # - this is not that high to not detect inacuracy
    # - memory can go like X/X+N due to some background allocations
    # - memory can go like X/X+N/X, so at least 2 changes
    changes_allowed = 3
    # if number of samples is large enough, use 10% from them
    # (actually most of the time there will be only few changes, it was made 10% to avoid flackiness)
    changes_allowed_auto = int(len(memory) * 0.1)
    changes_allowed = max(changes_allowed_auto, changes_allowed)

    changed = len(set(memory))
    logging.info(
        "Changes: allowed=%s, actual=%s, sample=%s",
        changes_allowed,
        changed,
        len(memory),
    )
    assert changed < changes_allowed


def test_http():
    memory = []
    memory.append(get_MemoryTracking())
    for _ in range(100):
        http_query(sample_query)
        memory.append(get_MemoryTracking())
    check_memory(memory)


def test_tcp_multiple_sessions():
    memory = []
    memory.append(get_MemoryTracking())
    for _ in range(100):
        query(sample_query)
        memory.append(get_MemoryTracking())
    check_memory(memory)


def test_tcp_single_session():
    memory = []
    memory.append(get_MemoryTracking())
    sample_queries = [
        sample_query,
        "SELECT metric, value FROM system.metrics WHERE metric = 'MemoryTracking'",
    ] * 100
    rows = query(";".join(sample_queries))
    memory = rows.split("\n")
    memory = filter(lambda x: x.startswith("MemoryTracking"), memory)
    memory = map(lambda x: x.split("\t")[1], memory)
    memory = [*memory]
    check_memory(memory)


def corrected_query(*args, **kwargs):
    if "params" not in kwargs:
        kwargs["params"] = query_settings
    else:
        kwargs["params"].update(query_settings)
    return node_corrected.http_query(*args, **kwargs)


def get_corrected_metric(metric):
    return int(
        corrected_query(f"SELECT value FROM system.metrics WHERE metric = '{metric}'")
    )


MiB = 1024 * 1024


def skip_unless_correction_observable():
    # The correction is done by the background memory worker, and the worker does not even
    # start when it has no source of memory usage information (neither cgroups nor
    # jemalloc), in which case there is nothing to check here.
    if not node_corrected.contains_in_log("Starting background memory thread"):
        pytest.skip("the memory worker has no source of memory usage information")


def sample_corrected_metrics():
    # The period of the worker is 50 ms when it reads from cgroups and 100 ms when it reads
    # from jemalloc, so one tick certainly happens before the samples are taken, and the
    # samples describe the state after the last query rather than during it.
    time.sleep(0.5)
    return (
        get_corrected_metric("MemoryTracking"),
        get_corrected_metric("MemoryTrackingUncorrected"),
    )


# Allocates and frees several hundred megabytes: 200'000 strings of 1000 bytes gathered into
# arrays in the arenas of the aggregation states, plus the copies of them in the result column.
big_query = "SELECT groupArray(repeat('a', 1000)) FROM numbers(200000) GROUP BY number % 10 FORMAT Null"


def test_uncorrected_counter_is_preserved():
    skip_unless_correction_observable()

    # `MemoryTrackingUncorrected` is the plain counter of allocations, refreshed by the worker
    # on every tick: it is what `MemoryTracking` would have been with no corrections applied.
    # The queries free everything they allocate, so it returns to its baseline after every
    # query, which is exactly the property the tests above check for `MemoryTracking` with the
    # correction disabled, and it must not run away: a drift there would mean the correction
    # rewrites `MemoryTracking` while the raw counter is lost instead of being preserved.
    tracking, uncorrected = sample_corrected_metrics()
    assert tracking > 0
    assert uncorrected > 0

    samples = [uncorrected]
    for _ in range(10):
        corrected_query(big_query)
        _, uncorrected = sample_corrected_metrics()
        samples.append(uncorrected)

    logging.info("MemoryTrackingUncorrected: %s", samples)
    spread = max(samples) - min(samples)
    logging.info("MemoryTrackingUncorrected spread: %s", spread)
    # Ten queries freed gigabytes in total: an unrefreshed or lost counter would either not
    # move at all (the metric is refreshed only by the worker, so a value that never changes
    # between samples would go unnoticed here, see the next test) or run away by hundreds of
    # megabytes. Allow a few megabytes for what the server allocates in the background.
    assert spread < 16 * MiB


def test_correction_follows_measurement():
    skip_unless_correction_observable()

    # Only the worker can move `MemoryTracking` without any allocation or deallocation
    # happening, and only a change of the memory the process really uses can make it do so.
    # jemalloc keeps the pages a query freed as dirty pages for a while (`dirty_decay_ms`),
    # and `SYSTEM JEMALLOC PURGE` gives them back to the OS at once. Neither event goes
    # through the memory tracker, so with the correction disabled `MemoryTracking` cannot
    # react to either of them, while with the correction enabled it has to.
    if (
        corrected_query(
            "SELECT value FROM system.build_options WHERE name = 'USE_JEMALLOC'"
        ).strip()
        != "1"
    ):
        pytest.skip("the check needs jemalloc to retain freed pages and to purge them")
    if node_corrected.contains_in_log("Low memory system detected"):
        pytest.skip("jemalloc does not retain freed pages on a low memory system")

    # Start from a state without retained pages, so the baseline is comparable to the state
    # after the purge below.
    node_corrected.query("SYSTEM JEMALLOC PURGE")
    tracking_before, uncorrected_before = sample_corrected_metrics()
    gap_before = tracking_before - uncorrected_before

    corrected_query(big_query)
    tracking_after, uncorrected_after = sample_corrected_metrics()
    gap_after = tracking_after - uncorrected_after

    node_corrected.query("SYSTEM JEMALLOC PURGE")
    tracking_purged, uncorrected_purged = sample_corrected_metrics()
    gap_purged = tracking_purged - uncorrected_purged

    logging.info(
        "MemoryTracking: before %s, after %s, purged %s",
        tracking_before,
        tracking_after,
        tracking_purged,
    )
    logging.info(
        "MemoryTrackingUncorrected: before %s, after %s, purged %s",
        uncorrected_before,
        uncorrected_after,
        uncorrected_purged,
    )
    logging.info(
        "Gap: before %s, after %s, purged %s", gap_before, gap_after, gap_purged
    )

    # The raw counter does not see the retained pages nor the purge.
    assert abs(uncorrected_after - uncorrected_before) < 16 * MiB
    assert abs(uncorrected_purged - uncorrected_before) < 16 * MiB

    # With the correction disabled, `MemoryTracking` would move by the same few megabytes as
    # the raw counter, and the gap between the two would be the same constant in all three
    # samples. With the correction enabled, the query leaves hundreds of megabytes of dirty
    # pages behind (about 700 MiB when measured), which the measurement includes and the
    # counter does not, and the purge takes them away again. `dirty_decay_ms` is 5 seconds,
    # so half a second later most of the pages are still retained.
    assert gap_after - gap_before > 64 * MiB
    assert gap_after - gap_purged > 64 * MiB
    assert tracking_after - tracking_purged > 64 * MiB
