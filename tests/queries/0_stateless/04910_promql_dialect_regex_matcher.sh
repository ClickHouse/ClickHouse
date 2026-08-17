#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `=~` and `!~` are tokens the SQL lexer does not know. Under the promql dialect the query text
# is parsed by the PromQL grammar, so a matcher must not be rejected as an unrecognized token.
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1700000000, 3), 30)]),
    ('up', map('instance', 'host2'), [(toDateTime64(1700000000, 3), 10)]),
    ('up', map('instance', 'host3'), [(toDateTime64(1700000000, 3), 20)]);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql \
        --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- =~ selects the matching series"
promql_client -q 'up{instance=~"host2|host3"}' | cut -f1,3 | LC_ALL=C sort

echo "-- !~ selects the complement"
promql_client -q 'up{instance!~"host2|host3"}' | cut -f1,3 | LC_ALL=C sort

echo "-- a matcher inside a function argument"
promql_client -q 'sum(up{instance=~"host2|host3"})' | cut -f1,3 | LC_ALL=C sort

echo "-- the equality matcher still works"
promql_client -q 'up{instance="host1"}' | cut -f1,3 | LC_ALL=C sort

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
