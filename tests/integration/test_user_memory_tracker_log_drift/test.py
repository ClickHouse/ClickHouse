"""System log elements (query_log, query_views_log, ...) are built in the query thread but destroyed
later by the SystemLog flush thread, which cannot credit the bytes back to the per-user memory
tracker. If element memory is charged to the user (instead of only to the global tracker, see
SystemLogBase::add), the per-user tracker drifts up by the size of every logged element and never
comes back down while the user has at least one running query, eventually tripping
max_memory_usage_for_user with phantom memory.

The test measures the drift differentially: two users run the identical insert workload, one with
query logging disabled and one with logging enabled and a large log_comment (which each query_log
element owns twice: the comment field and its copy inside the logged Settings). The difference
between their tracker floors isolates the logging contribution, cancelling drift from unrelated
sources (part metadata etc.). Each user is kept alive by a background query because the tracker is
reset when the user's query count drops to zero.
"""

import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

INSERTS = 50
LOG_COMMENT = "x" * 100_000
# Broken accounting leaks >= 2 elements x ~200 KB of comment per insert (~20 MB total); fixed
# accounting leaves only sub-MB noise from the two floors.
LEAK_THRESHOLD = 5 * 1024 * 1024


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE t (k UInt64, s String) ENGINE = MergeTree ORDER BY k")
        node.query(
            "CREATE MATERIALIZED VIEW mv1 ENGINE = SummingMergeTree ORDER BY k "
            "AS SELECT k, count() AS c FROM t GROUP BY k"
        )
        node.query(
            "CREATE MATERIALIZED VIEW mv2 ENGINE = SummingMergeTree ORDER BY s "
            "AS SELECT s, count() AS c FROM t GROUP BY s"
        )
        yield cluster
    finally:
        cluster.shutdown()


def user_tracker_floor(user, log_settings):
    node.query(f"CREATE USER {user} IDENTIFIED WITH no_password")
    node.query(f"GRANT SELECT, INSERT, CREATE TEMPORARY TABLE ON *.* TO {user}")

    # The per-user memory tracker is reset whenever the user's query count drops to zero, so keep
    # one query of this user running across the whole workload.
    sleeper = node.get_query_request(
        "SELECT sleepEachRow(1) FROM numbers(600) "
        "SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 10000000000 "
        "FORMAT Null",
        user=user,
    )
    try:
        assert_eq_with_retry(
            node, f"SELECT count() FROM system.processes WHERE user = '{user}'", "1"
        )

        for i in range(INSERTS):
            node.query(
                f"INSERT INTO t VALUES ({i}, 'value-{i}')",
                user=user,
                settings=log_settings,
            )

        # Drain the SystemLog queues so queued elements are destroyed before sampling: the floor
        # must reflect only what stays charged to the user permanently.
        node.query("SYSTEM FLUSH LOGS")

        floor = int(
            node.query(
                f"SELECT memory_usage FROM system.user_processes WHERE user = '{user}'"
            )
        )
    finally:
        node.query(f"KILL QUERY WHERE user = '{user}' SYNC")
        sleeper.get_answer_and_error()
    return floor


def test_log_elements_not_charged_to_user_tracker(started_cluster):
    floor_nolog = user_tracker_floor(
        "user_nolog",
        {"log_queries": 0, "log_query_views": 0, "log_profile_events": 0},
    )
    floor_log = user_tracker_floor(
        "user_log",
        {
            "log_queries": 1,
            "log_query_views": 1,
            "log_profile_events": 1,
            "log_comment": LOG_COMMENT,
        },
    )

    # Guard against silently measuring nothing (e.g. query_log disabled on the instance).
    logged = int(
        node.query(
            "SELECT count() FROM system.query_log "
            "WHERE user = 'user_log' AND query_kind = 'Insert' AND type = 'QueryFinish'"
        )
    )
    assert logged >= INSERTS

    leak = floor_log - floor_nolog
    logging.info(
        "user tracker floors: nolog=%d log=%d leak=%d", floor_nolog, floor_log, leak
    )
    assert leak < LEAK_THRESHOLD, (
        f"system log elements leaked {leak} bytes onto the user memory tracker "
        f"(floor with logging {floor_log}, without {floor_nolog})"
    )
