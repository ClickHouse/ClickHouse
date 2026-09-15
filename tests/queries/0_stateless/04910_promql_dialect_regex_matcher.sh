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
    ('up', map('instance', 'host3'), [(toDateTime64(1700000000, 3), 20)]),
    ('set', map('instance', 'host1'), [(toDateTime64(1700000000, 3), 3)]),
    ('set', map('instance', 'host2'), [(toDateTime64(1700000000, 3), 1)]),
    ('set', map('instance', 'host3'), [(toDateTime64(1700000000, 3), 2)]);
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

echo "-- a metric named 'set' is not a SET statement: matchers still parse as PromQL"
promql_client -q 'set{instance=~"host2|host3"}' | cut -f1,3 | LC_ALL=C sort

echo "-- bare 'set' is a metric too"
promql_client -q 'set' | cut -f1,3 | LC_ALL=C sort

echo "-- a metric named 'set' followed by PromQL bareword operators/modifiers"
promql_client -q 'set or up{instance=~"host2|host3"}' | cut -f1,3 | LC_ALL=C sort
promql_client -q 'set offset 0s' | cut -f1,3 | LC_ALL=C sort

echo "-- a real SET statement still works under the dialect"
promql_client -q 'SET max_threads = 1' && echo OK

echo "-- SET ROLE is dispatched as a role statement, not a ROLE = true setting shorthand"
promql_client -q 'SET ROLE NONE' && echo OK

# The dialect lexer keeps `~` as an ordinary token, so the message is a plain syntax error rather
# than the SQL lexer's "Unrecognized token"; committed SETs must still fail SQL-side, not as PromQL.
echo "-- a malformed SET still gets an SQL-side syntax error"
promql_client -q 'SET max_threads = ~1' 2>&1 | grep -o "SYNTAX_ERROR" | head -1

echo "-- a malformed SET without = still gets an SQL-side syntax error"
promql_client -q 'SET max_threads ~1' 2>&1 | grep -o "SYNTAX_ERROR" | head -1

# Without a setting name there is nothing to tell `SET <junk>` apart from a malformed query over a
# metric named `set`, so the active dialect reports it. Committing to SET on the error token instead
# would reject the `#` comment below, which is valid PromQL (PromQLLexer.g4: SL_COMMENT).
echo "-- a malformed SET without a setting name is reported by the dialect grammar"
promql_client -q 'SET ~' 2>&1 | grep -o "CANNOT_PARSE_PROMQL_QUERY"
promql_client -q 'SET ~1' 2>&1 | grep -o "CANNOT_PARSE_PROMQL_QUERY"

# The raw-text prescan advances token by token; only the terminal ErrorMaxQuerySizeExceeded stops
# it once the lexer crosses max_query_size, so an oversized query must error rather than hang.
echo "-- an oversized query hits the max_query_size guard"
promql_client --max_query_size=10 -q 'metric{instance="host1"}' 2>&1 | grep -o "Max query size exceeded" | head -1

echo "-- a '#' comment after a metric named 'set' still parses as PromQL"
promql_client -q 'set # trailing comment
' | cut -f1,3 | LC_ALL=C sort

$CLICKHOUSE_CLIENT -q "DROP TABLE ts"
