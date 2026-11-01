#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# Per-request lines of the Prometheus handlers and the PromQL and SQL of prometheusQuery() are logged at
# the debug level, not at the information level.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "CREATE TABLE ts ENGINE = TimeSeries"

for level in information debug trace; do
    echo "send_logs_level=$level"
    client=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=$level"}
    $client --allow_experimental_time_series_table 1 --query "SELECT * FROM prometheusQuery(ts, 'up', 1000) FORMAT Null" 2>&1 |
        grep -oE '<[A-Za-z]+> StoragePrometheusQuery: (Building SQL to evaluate promql|Will execute query)' | sort -u
done

$CLICKHOUSE_CLIENT --query "DROP TABLE ts"

echo "metrics request"
user_agent="metrics_request_${CLICKHOUSE_DATABASE}_$(random_str 10)"
$CLICKHOUSE_CURL -sS -A "$user_agent" "$CLICKHOUSE_URL_PROMETHEUS" > /dev/null
# Retry, because the log entry may reach system.text_log only after the response.
for _ in {1..60}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    # system.text_log can be really big.
    level=$($CLICKHOUSE_CLIENT --query "
        SELECT level FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND logger_name = 'PrometheusRequestHandler'
            AND message = 'Handling metrics request from $user_agent'
        SETTINGS max_rows_to_read = 0")
    [ -n "$level" ] && break
    sleep 0.5
done
echo "$level"
