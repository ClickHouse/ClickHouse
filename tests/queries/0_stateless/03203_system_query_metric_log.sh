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
    # We calculate the diff of each row with its previous row to check whether the intervals at which
    # data is collected is right. The first row is always skipped because the diff is 0. The same for the
    # last row, which is skipped because doesn't contain a full interval.
    $CLICKHOUSE_CLIENT --max_threads=1 -m -q """
    WITH diff AS (
        SELECT
            row_number() OVER () AS row,
            count() OVER () as total_rows,
            event_time_microseconds,
            first_value(event_time_microseconds) OVER (ORDER BY event_time_microseconds ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING) as prev,
            dateDiff('ms', prev, event_time_microseconds) AS diff
        FROM system.query_metric_log
        WHERE query_id = '${query_prefix}_${interval}'
        ORDER BY event_time_microseconds
        OFFSET 1
    )
    SELECT count() BETWEEN ((ceil(2500 / $interval) - 2) * 0.9) AND ((ceil(2500 / $interval) - 2) * 1.1), avg(diff) BETWEEN $interval * 0.9 AND $interval * 1.1, stddevPopStable(diff) BETWEEN 0 AND $interval * 0.5 FROM diff WHERE row < total_rows
    """
}

check_log 1000
check_log 1234
check_log 123

# query_metric_log_interval=0 disables the collection altogether
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE query_id = '${query_prefix}_0'"""

# a quick query that takes less than query_metric_log_interval is never collected
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE query_id = '${query_prefix}_fast'"""

# a query that takes more than query_metric_log_interval is collected including the final row
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE query_id = '${query_prefix}_1000'"""