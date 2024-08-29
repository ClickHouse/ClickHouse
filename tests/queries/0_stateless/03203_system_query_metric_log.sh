#!/bin/bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT --query-id="${query_prefix}_1000" -q "SELECT sleep(3) + sleep(2) FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_1234" -q "SELECT sleep(3) + sleep(2) SETTINGS query_metric_log_interval=1234 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_123" -q "SELECT sleep(3) + sleep(2) SETTINGS query_metric_log_interval=123 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_0" -q "SELECT sleep(3) + sleep(2) SETTINGS query_metric_log_interval=0 FORMAT Null" &

wait

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

function check_log()
{
    interval=$1
    $CLICKHOUSE_CLIENT --max_threads=1 -m -q """
    WITH diff AS (
        SELECT
            event_time_microseconds,
            first_value(event_time_microseconds) OVER (ORDER BY event_time_microseconds ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING) as prev,
            dateDiff('ms', prev, event_time_microseconds) AS diff
        FROM system.query_metric_log
        WHERE query_id = '${query_prefix}_${interval}'
        ORDER BY event_time_microseconds
        OFFSET 1
    )
    SELECT count() BETWEEN least(5000 / $interval - 2, 5000 / $interval * 0.9) AND (5000 / $interval - 1) * 1.1, avg(diff) BETWEEN $interval * 0.9 AND $interval * 1.1, stddevPopStable(diff) BETWEEN 0 AND $interval * 0.2 FROM diff
    """
}

check_log 1000
check_log 1234
check_log 123

# query_metric_log_interval=0 disables the collection altogether
$CLICKHOUSE_CLIENT -m -q """SELECT count() FROM system.query_metric_log WHERE query_id = '${query_prefix}_0'"""