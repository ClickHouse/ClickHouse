#!/bin/bash

# This test depends A LOT on timing, so it's very sensitive when the system is overloaded. For that
# reason, margins of 20% were added initially. They've been increased over time in an attempt to
# make it more stable, even though it's never going to be deterministically perfect.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT --query-id="${query_prefix}_1000" -q "SELECT sleep(2.5) SETTINGS enable_parallel_replicas=0 FORMAT Null" & # CI may inject enable_parallel_replicas=1; parallel replicas distribute sleep so initiator finishes quickly, producing too few metric log events
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_400" -q "SELECT sleep(2.5) SETTINGS query_metric_log_interval=400, enable_parallel_replicas=0 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_123" -q "SELECT sleep(2.5) SETTINGS query_metric_log_interval=123, enable_parallel_replicas=0 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_0" -q "SELECT sleep(2.5) SETTINGS query_metric_log_interval=0, enable_parallel_replicas=0 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_fast" -q "SELECT sleep(0.1) SETTINGS query_metric_log_interval=999999, enable_parallel_replicas=0 FORMAT Null" &

wait

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log, query_metric_log"

function check_log()
{
    interval=$1

    # Check that the amount of events collected is correct, leaving a 80% of margin.
    $CLICKHOUSE_CLIENT -m -q """
        SELECT '--Interval $interval: check that amount of events is correct';
        SELECT
            count() BETWEEN ((ceil(2500 / $interval) - 1) * 0.2) AND ((ceil(2500 / $interval) + 1) * 1.8)
        FROM system.query_metric_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_prefix}_${interval}'
    """

    # Check that the first event contains information from the beginning of the query.
    # Notice the rest of the events won't contain these because the diff will be 0.
    $CLICKHOUSE_CLIENT -m -q """
        SELECT '--Interval $interval: check that the Query, SelectQuery and InitialQuery values are correct for the first event';
        SELECT ProfileEvent_Query = 1 AND ProfileEvent_SelectQuery = 1 AND ProfileEvent_InitialQuery = 1
        FROM system.query_metric_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_prefix}_${interval}'
        ORDER BY event_time_microseconds
        LIMIT 1
    """

    # Also check that it contains some data that we know it's going to be there.
    # Notice the Sleep events can be in any of the rows, not only in the first one.
    $CLICKHOUSE_CLIENT -m -q """
        SELECT '--Interval $interval: check that the SleepFunctionCalls, SleepFunctionMilliseconds and ProfileEvent_SleepFunctionElapsedMicroseconds are correct';
        SELECT  sum(ProfileEvent_SleepFunctionCalls) = 1 AND
                sum(ProfileEvent_SleepFunctionMicroseconds) = 2500000 AND
                sum(ProfileEvent_SleepFunctionElapsedMicroseconds) = 2500000 AND
                sum(ProfileEvent_Query) = 1 AND
                sum(ProfileEvent_SelectQuery) = 1 AND
                sum(ProfileEvent_InitialQuery) = 1
        FROM system.query_metric_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_prefix}_${interval}'
    """
}

check_log 1000
check_log 400
check_log 123

# query_metric_log_interval=0 disables the collection altogether
$CLICKHOUSE_CLIENT -m -q """
    SELECT '--Check that a query_metric_log_interval=0 disables the collection';
    SELECT count() == 0 FROM system.query_metric_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_prefix}_0'
"""

# a quick query that takes less than query_metric_log_interval is never collected
$CLICKHOUSE_CLIENT -m -q """
    SELECT '--Check that a query which execution time is less than query_metric_log_interval is never collected';
    SELECT count() == 0 FROM system.query_metric_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_prefix}_fast'
"""

# A long-running query must emit a final `system.query_metric_log` row from
# `QueryMetricLog::finishQuery`. On the TCP path exercised by this test,
# `BlockIO::onFinish` captures `finish_time` once and passes it through the
# finish callback to `logQueryFinishImpl`, which uses the same timestamp for
# both `system.query_log` `QueryFinish` and `logQueryMetricLogFinish`. A
# later periodic `system.query_metric_log` row may also exist because a
# periodic `collectMetric` can sample a live `ProcessList` entry before
# `finishQuery` marks the query finished. The correct invariant is therefore
# existence of a metric row at the exact `QueryFinish` timestamp, not
# `max(event_time_microseconds) = QueryFinish`.
$CLICKHOUSE_CLIENT -m -q """
    SELECT '--Check that there is a final event when queries finish';
    WITH
    (
        SELECT event_time_microseconds
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query_id = '${query_prefix}_1000'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AS finish_time
    SELECT countIf(event_time_microseconds = finish_time) > 0
    FROM system.query_metric_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND query_id = '${query_prefix}_1000'
"""
