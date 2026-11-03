#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

# The PromQL and SQL of prometheusQuery() are logged at the debug level, not at the information level.

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
