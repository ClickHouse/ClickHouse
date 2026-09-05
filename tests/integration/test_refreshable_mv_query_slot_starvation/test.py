import json
import logging
import time

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/small_pool.xml"])

SLOW_VIEWS = 10
FAST_VIEWS = 100
SLOW_DURATION_SECONDS = 600
MAX_FAST_STALL_SECONDS = 10


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def rows(query):
    return [
        json.loads(line)
        for line in node.query(query + " FORMAT JSONEachRow", timeout=10).splitlines()
    ]


def slow_state():
    return rows(
        "SELECT view, status, exception, last_success_duration_ms "
        "FROM system.view_refreshes WHERE database='starvation' "
        "AND startsWith(view, 'slow_') ORDER BY view"
    )


def diagnostics():
    return {
        "slow_views": slow_state(),
        "fast_views": rows(
            "SELECT view, status, exception FROM system.view_refreshes "
            "WHERE database='starvation' AND startsWith(view, 'fast_') ORDER BY view"
        ),
        "pool": rows(
            "SELECT table, log_name, executing, elapsed_ms "
            "FROM system.background_schedule_pool "
            "WHERE pool='schedule' AND database='starvation' AND executing"
        ),
    }


@pytest.mark.timeout(900)
def test_queued_long_refreshes_do_not_starve_fast_refreshes():
    node.query("CREATE DATABASE starvation")
    try:
        node.query(
            "CREATE RESOURCE query (QUERY);"
            "CREATE WORKLOAD all;"
            "CREATE WORKLOAD slow IN all SETTINGS max_concurrent_queries=1;"
            "CREATE WORKLOAD fast IN all;"
            "CREATE TABLE starvation.slow_results (view_id UInt16, slept UInt64) ENGINE Memory;"
            "CREATE TABLE starvation.fast_results (view_id UInt16) ENGINE Memory;"
        )

        for view_id in range(FAST_VIEWS):
            node.query(
                f"CREATE MATERIALIZED VIEW starvation.fast_{view_id} "
                "REFRESH EVERY 1 SECOND SETTINGS refresh_retries=0 APPEND "
                "TO starvation.fast_results EMPTY "
                f"AS SELECT toUInt16({view_id}) AS view_id "
                "SETTINGS workload='fast', max_threads=1"
            )

        for view_id in range(SLOW_VIEWS):
            node.query(
                f"CREATE MATERIALIZED VIEW starvation.slow_{view_id} "
                "REFRESH AFTER 1 DAY SETTINGS refresh_retries=0 APPEND "
                "TO starvation.slow_results EMPTY "
                f"AS SELECT toUInt16({view_id}) AS view_id, sum(sleepEachRow(1)) AS slept "
                f"FROM numbers({SLOW_DURATION_SECONDS}) "
                "SETTINGS workload='slow', max_threads=1, max_block_size=1, "
                "max_execution_time=0"
            )

        # Verify the actual pool cap, not just the XML configuration.
        assert node.query(
            "SELECT value FROM system.metrics WHERE metric='BackgroundSchedulePoolSize'"
        ).strip() == "5"

        for view_id in range(SLOW_VIEWS):
            node.query(f"SYSTEM REFRESH VIEW starvation.slow_{view_id}")

        admission_deadline = time.monotonic() + 30
        while True:
            states = slow_state()
            assert not any(view["exception"] for view in states), states
            running = [view for view in states if view["status"] == "Running"]
            waiting = [view for view in states if view["status"] == "WaitingForResource"]
            if len(running) == 1 and len(waiting) == SLOW_VIEWS - 1:
                break
            assert time.monotonic() < admission_deadline, diagnostics()
            time.sleep(0.1)

        first_running = running[0]["view"]
        assert node.query(
            "SELECT count() FROM system.background_schedule_pool "
            "WHERE pool='schedule' AND database='starvation' "
            "AND startsWith(table, 'slow_') AND log_name='RefreshExec' AND executing"
        ).strip() == "1"

        # Observe every view individually. Aggregate throughput alone could hide starvation
        # of some views while others keep refreshing. The schedule remains one second;
        # ten seconds is the maximum tolerated lack of progress, including CI scheduling jitter.
        started = time.monotonic()
        previous = {view_id: 0 for view_id in range(FAST_VIEWS)}
        last_progress = {view_id: started for view_id in range(FAST_VIEWS)}
        maximum_stall = 0.0
        handoff_seen_at = None
        baseline = None

        while True:
            counts = {
                int(row["view_id"]): int(row["refreshes"])
                for row in rows(
                    "SELECT view_id, count() AS refreshes "
                    "FROM starvation.fast_results GROUP BY view_id"
                )
            }
            now = time.monotonic()
            if baseline is None:
                baseline = {view_id: counts.get(view_id, 0) for view_id in previous}
                previous = baseline.copy()
            for view_id in previous:
                stall = now - last_progress[view_id]
                maximum_stall = max(maximum_stall, stall)
                assert stall <= MAX_FAST_STALL_SECONDS, (
                    f"fast_{view_id} made no observed progress for {stall:.1f}s",
                    counts.get(view_id, 0),
                    diagnostics(),
                )
                if counts.get(view_id, 0) > previous[view_id]:
                    last_progress[view_id] = now
                previous[view_id] = counts.get(view_id, 0)

            states = slow_state()
            assert len(states) == SLOW_VIEWS, states
            assert not any(view["exception"] for view in states), diagnostics()
            completed = [
                view for view in states if view["last_success_duration_ms"] is not None
            ]
            waiting = [view for view in states if view["status"] == "WaitingForResource"]
            # Releasing the slot and publishing the finished refresh state are separate steps.
            # The next view may already be admitted before the previous completion is visible.
            assert len(waiting) >= SLOW_VIEWS - 2, diagnostics()
            if completed:
                assert len(completed) == 1, states
                assert completed[0]["view"] == first_running, states
                assert int(completed[0]["last_success_duration_ms"]) >= 600_000, states
                next_running = [
                    view for view in states
                    if view["status"] == "Running" and view["view"] != first_running
                ]
                if len(next_running) == 1 and len(waiting) == SLOW_VIEWS - 2:
                    if handoff_seen_at is None:
                        handoff_seen_at = now
                    # Keep checking fast-view progress after the next long refresh starts.
                    if now - handoff_seen_at >= MAX_FAST_STALL_SECONDS:
                        break
            assert now - started < SLOW_DURATION_SECONDS + 120, diagnostics()
            time.sleep(1)

        refreshes = [previous[view_id] - baseline[view_id] for view_id in previous]
        assert min(refreshes) > 0
        logging.info(
            "Pool starvation test: duration=%.1fs, fast_views=%d, "
            "refreshes_per_fast_view_min=%d, max=%d, max_observed_stall=%.2fs",
            time.monotonic() - started, FAST_VIEWS, min(refreshes), max(refreshes), maximum_stall,
        )
    finally:
        # Cancel the remaining ten-minute queries; do not run the one-slot queue for 100 minutes.
        node.query("SYSTEM STOP VIEWS", timeout=30)
        node.query("DROP DATABASE starvation SYNC", timeout=30)
        node.query("DROP WORKLOAD IF EXISTS fast; DROP WORKLOAD IF EXISTS slow;")
        node.query("DROP WORKLOAD IF EXISTS all; DROP RESOURCE IF EXISTS query;")
