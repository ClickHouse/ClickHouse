#!/bin/bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

readonly query_prefix=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT --query-id="${query_prefix}_1000" -q "SELECT sleep(3) FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_123" -q "SELECT sleep(3) SETTINGS query_log_metric_interval=123 FORMAT Null" &
$CLICKHOUSE_CLIENT --query-id="${query_prefix}_47" -q "SELECT sleep(3) SETTINGS query_log_metric_interval=47 FORMAT Null" &

wait

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"

function check_log()
{
    interval=$1
    $CLICKHOUSE_CLIENT -m -q """
    WITH diff AS (
        SELECT dateDiff('ms', first_value(event_time_microseconds) OVER (ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING), event_time_microseconds) AS diff
        FROM system.query_log_metric
        WHERE query_id = '${query_prefix}_${interval}'
        ORDER BY 1
        OFFSET 1
    )
    SELECT count(), avg(diff) BETWEEN $interval * 0.90 AND $interval * 1.10, stddevSampStable(diff) BETWEEN 0 AND $interval * 0.2 FROM diff
    """
}

check_log 1000
check_log 123
check_log 47
