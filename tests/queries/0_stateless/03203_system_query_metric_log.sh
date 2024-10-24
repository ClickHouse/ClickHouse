#!/bin/bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT --query-id="${query_prefix}_1000" -q "SELECT sleep(2.5) FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_1234" -q "SELECT sleep(2.5) SETTINGS query_metric_log_interval=1234 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_123" -q "SELECT sleep(2.5) SETTINGS query_metric_log_interval=123 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_0" -q "SELECT sleep(2.5) SETTINGS query_metric_log_interval=0 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_fast" -q "SELECT sleep(0.1) FORMAT Null" &

wait

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

function check_log()
{
    interval=$1

    # We calculate the diff of each row with its previous row to check whether the intervals at
    # which data is collected is right. The first row is always skipped because the diff with the
    # preceding one (itself) is 0. The last row is also skipped, because it doesn't contain a full
    # interval.
    $CLICKHOUSE_CLIENT --max_threads=1 -m -q """
    WITH diff AS (
        SELECT
            row_number() OVER () AS row,
            count() OVER () as total_rows,
            event_time_microseconds,
            first_value(event_time_microseconds) OVER (ORDER BY event_time_microseconds ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING) as prev,
            dateDiff('ms', prev, event_time_microseconds) AS diff
        FROM system.query_metric_log
        WHERE event_date >= yesterday() AND query_id = '${query_prefix}_${interval}'
        ORDER BY event_time_microseconds
        OFFSET 1
    )
    SELECT if(count() BETWEEN ((ceil(2500 / $interval) - 2) * 0.8) AND ((ceil(2500 / $interval) - 2) * 1.2), 'number_of_metrics_${interval}_ok', 'number_of_metrics_${interval}_error'),
           if(avg(diff) BETWEEN $interval * 0.8 AND $interval * 1.2, 'timestamp_diff_in_metrics_${interval}_ok', 'timestamp_diff_in_metrics_${interval}_error')
    FROM diff WHERE row < total_rows
    """

    # Check that the first event contains information from the beginning of the query.
    # Notice the rest of the events won't contain these because the diff will be 0.
    $CLICKHOUSE_CLIENT -m -q """
        SELECT if(ProfileEvent_Query = 1 AND ProfileEvent_SelectQuery = 1 AND ProfileEvent_InitialQuery = 1, 'initial_data_${interval}_ok', 'initial_data_${interval}_error')
        FROM system.query_metric_log
        WHERE event_date >= yesterday() AND query_id = '${query_prefix}_${interval}'
        ORDER BY event_time_microseconds
        LIMIT 1
    """

    # Also check that it contains some data that we know it's going to be there.
    # Notice the Sleep events can be in any of the rows, not only in the first one.
    $CLICKHOUSE_CLIENT -m -q """
        SELECT if(sum(ProfileEvent_SleepFunctionCalls) = 1 AND
                  sum(ProfileEvent_SleepFunctionMicroseconds) = 2500000 AND
                  sum(ProfileEvent_SleepFunctionElapsedMicroseconds) = 2500000 AND
                  sum(ProfileEvent_Query) = 1 AND
                  sum(ProfileEvent_SelectQuery) = 1 AND
                  sum(ProfileEvent_InitialQuery) = 1,
                  'data_${interval}_ok', 'data_${interval}_error')
        FROM system.query_metric_log
        WHERE event_date >= yesterday() AND query_id = '${query_prefix}_${interval}'
    """
}

check_log 1000
check_log 1234
check_log 123

# query_metric_log_interval=0 disables the collection altogether
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE event_date >= yesterday() AND query_id = '${query_prefix}_0'"""

# a quick query that takes less than query_metric_log_interval is never collected
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE event_date >= yesterday() AND query_id = '${query_prefix}_fast'"""

# a query that takes more than query_metric_log_interval is collected including the final row
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE event_date >= yesterday() AND query_id = '${query_prefix}_1000'"""
