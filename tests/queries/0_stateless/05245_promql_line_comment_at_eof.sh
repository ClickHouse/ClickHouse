#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4, which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "CREATE TABLE ts ENGINE = TimeSeries"

echo "-- PromQL line comments may end at EOF"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 --query "up # trailing comment"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "SELECT * FROM prometheusQuery(ts, 'up # trailing comment', 1700000000)"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 --query "up #!comment"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "SELECT * FROM prometheusQuery(ts, 'up #!comment', 1700000000)"

echo "-- EOF comments must match the shared SQL lexer prefix contract"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 --query "up #comment" 2>&1 | grep -o -E "SYNTAX_ERROR|CANNOT_PARSE_PROMQL_QUERY" | head -n 1
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "SELECT * FROM prometheusQuery(ts, 'up #comment', 1700000000)" 2>&1 | grep -o -E "SYNTAX_ERROR|CANNOT_PARSE_PROMQL_QUERY" | head -n 1

echo "-- A bare '#' at EOF is an error in both PromQL entry points"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 --query "up #" 2>&1 | grep -o -E "SYNTAX_ERROR|CANNOT_PARSE_PROMQL_QUERY" | head -n 1
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --query "SELECT * FROM prometheusQuery(ts, 'up #', 1700000000)" 2>&1 | grep -o -E "SYNTAX_ERROR|CANNOT_PARSE_PROMQL_QUERY" | head -n 1

$CLICKHOUSE_CLIENT --query "DROP TABLE ts"
