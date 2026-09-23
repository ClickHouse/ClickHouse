#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In PromQL `#` starts a comment until the end of the line, but the SQL lexer only knows a `#` comment
# when a space follows it, so a `;` in `up #keep ; comment` must not end the PromQL statement.
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1700000000, 3), 30)]),
    ('up', map('instance', 'host2'), [(toDateTime64(1700000000, 3), 10)]);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql \
        --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- a semicolon in a comment without a space after #"
promql_client -q $'up{instance="host1"} #keep ; comment\n' | cut -f1,3

echo "-- a semicolon in a comment with a space after #"
promql_client -q $'up{instance="host1"} # keep ; comment\n' | cut -f1,3

echo "-- an apostrophe in a comment"
promql_client -q $'sum(up) #don\'t ; stop\n' | cut -f1,3

echo "-- a semicolon in a string literal"
promql_client -q 'up{instance=~"host1;x|host2"}' | cut -f1,3

echo "-- a comment followed by the rest of the query"
promql_client -q $'sum(\n  up #keep ; comment\n)' | cut -f1,3

echo "-- a comment the SQL lexer reads past the end of the statement is an error, not a wrong split"
promql_client -q $'sum(up) #it\'s\n; sum(up) #x\'y\n' 2>&1 | grep -o -m1 'Cannot find the end of the PromQL statement'

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
