#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: `PAUSEABLE_ONCE` failpoint fires exactly once globally; a concurrent
#   run of this test could steal the failpoint pause from the other run.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

readonly failpoint="query_metric_log_pause_before_finish"
readonly query_id="query_metric_log_final_row_failpoint_${CLICKHOUSE_DATABASE}"

query_pid=""

cleanup()
{
    $CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 || true

    if [ -n "${query_pid:-}" ]; then
        wait "$query_pid" 2>/dev/null || true
    fi
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM ENABLE FAILPOINT ${failpoint}"

$CLICKHOUSE_CLIENT --query-id="${query_id}" -q "
    SELECT sleep(0.35)
    SETTINGS
        query_metric_log_interval=100,
        enable_parallel_replicas=0,
        log_queries=1,
        log_queries_min_type='QUERY_START',
        log_queries_min_query_duration_ms=0
    FORMAT Null" &

query_pid=$!

timeout 30 $CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

pause_time=$($CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SELECT now64(6)")
late_periodic_seen=0

for _ in {1..60}; do
    $CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM FLUSH LOGS query_metric_log"

    late_periodic_count=$($CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "
        SELECT count()
        FROM system.query_metric_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND query_id = '${query_id}'
          AND event_time_microseconds > toDateTime64('${pause_time}', 6)
    ")

    if [ "$late_periodic_count" -gt 0 ]; then
        late_periodic_seen=1
        break
    fi

    sleep 0.5
done

$CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM NOTIFY FAILPOINT ${failpoint}"
wait "$query_pid"
query_pid=""

$CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM DISABLE FAILPOINT ${failpoint}"
$CLICKHOUSE_CLIENT --query_metric_log_interval=0 -q "SYSTEM FLUSH LOGS query_log, query_metric_log"

echo "--Check that a periodic row overtook the paused final row"
echo "$late_periodic_seen"

$CLICKHOUSE_CLIENT --query_metric_log_interval=0 -m -q "
    SELECT '--Check that failpoint-delayed finish still emits final query metric row';
    WITH
    (
        SELECT event_time_microseconds
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query_id = '${query_id}'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AS finish_time
    SELECT countIf(event_time_microseconds = finish_time) > 0
    FROM system.query_metric_log
    WHERE event_date >= yesterday()
      AND event_time >= now() - 600
      AND query_id = '${query_id}'
"
