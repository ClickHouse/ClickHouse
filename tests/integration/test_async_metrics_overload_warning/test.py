import pytest

from helpers.cluster import ClickHouseCluster

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101759
#
# In `AsynchronousMetrics::update`, after `values.swap(new_values)` the warning helpers
# (`processWarningForMutationStats`, `processWarningForMemoryOverload`,
# `processWarningForCPUOverload`) were still passed `new_values`, which after the swap holds
# the *previous* cycle's metrics. As a result the overload warnings in `system.warnings`
# lagged one asynchronous-metrics cycle behind, and on the very first cycle `new_values` is
# empty, so no warning was emitted at all even when a threshold was exceeded. The fix passes
# the freshly computed `values` instead.
#
# The config below makes the memory-overload warning fire unconditionally on the first cycle
# (warn ratio 0, zero minimum duration) and disables the periodic refresh (very large update
# period), so exactly one asynchronous-metrics update -- the synchronous one done at server
# startup -- has run by the time the test queries `system.warnings`.
#
# With the bug, that first cycle reads the empty post-swap `new_values` and emits nothing;
# with the fix, the warning is present immediately after start.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/overload_warnings.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_memory_overload_warning_present_on_first_cycle(start_cluster):
    # Only the synchronous startup update has run so far (the periodic refresh is effectively
    # disabled), so this directly exercises the first-cycle behaviour.
    warnings = node.query(
        "SELECT message FROM system.warnings WHERE message LIKE 'High ClickHouse memory usage:%'"
    )
    assert "High ClickHouse memory usage" in warnings, (
        "The memory-overload warning was not emitted on the first asynchronous-metrics cycle. "
        "This is the regression fixed in this change: the warning helpers must read the freshly "
        "computed `values`, not the stale post-swap `new_values`."
    )
