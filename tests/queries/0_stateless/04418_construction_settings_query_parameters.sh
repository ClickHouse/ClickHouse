#!/usr/bin/env bash

# Query parameters (`{name:Type}` placeholders) must be substituted inside query-construction settings
# (`filter` / `select` / `order` / `sort`), not only in the base query. The snippets are parsed into ASTs
# after the base query's single `ReplaceQueryParameterVisitor` pass (which is gated on the query *text*
# containing `{`, of which the snippets are not part), so a second substitution pass is required. Covers
# the HTTP `filter` URL channel, an in-query `SETTINGS filter = '...'`, a `select` snippet, and the
# native client.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

http() {
    local params="$1"
    local query="$2"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&${params}" --data-binary "${query}"
}

echo "-- HTTP ?filter= with a query-parameter placeholder is substituted (number < 3 -> 0,1,2)"
http "param_n=3&filter=number%20%3C%20%7Bn%3AUInt64%7D" "SELECT number FROM numbers(10) ORDER BY number"

echo "-- HTTP in-query SETTINGS filter with a query-parameter placeholder (param_n=3)"
http "param_n=3" "SELECT number FROM numbers(10) ORDER BY number SETTINGS filter = 'number < {n:UInt64}'"

echo "-- HTTP ?select= snippet with a query-parameter placeholder (number + 100)"
http "param_add=100&select=number%20%2B%20%7Badd%3AUInt64%7D" "SELECT number FROM numbers(3) ORDER BY number"

echo "-- native client: in-query SETTINGS filter with --param_n=3"
${CLICKHOUSE_CLIENT} --param_n=3 -q "SELECT number FROM numbers(10) ORDER BY number SETTINGS filter = 'number < {n:UInt64}'"
