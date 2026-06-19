#!/usr/bin/env bash

# Construction settings that arrive via the HTTP request must be rejected on write-producing
# statements (`INSERT ... SELECT`), exactly as an in-query `SETTINGS` clause is (see `04345`). A
# `?limit=` / `?offset=` URL parameter is applied as a regular setting on the context, while a
# `?filter=` is kept in the HTTP combined-filter channel; neither appears as a `SETTINGS` node in the
# AST. `applyQueryConstructionSettings` skips write queries, so without the rejection the setting
# would be silently ignored and the insert would process all rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_src (x UInt64) ENGINE=Memory AS SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_dst (x UInt64) ENGINE=Memory"

# Run a write query via HTTP with extra URL parameters; report whether it was rejected.
run() {
    local params="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&${params}" --data-urlencode "query=${query}")
    if echo "$out" | grep -q "Query-construction settings"; then
        echo "rejected"
    else
        echo "NOT REJECTED: $out"
    fi
}

echo "-- ?limit= on INSERT ... SELECT is rejected"
run "limit=2" "INSERT INTO t_dst SELECT x FROM t_src"

echo "-- ?offset= on INSERT ... SELECT is rejected"
run "offset=3" "INSERT INTO t_dst SELECT x FROM t_src"

echo "-- ?filter= (combined-filter channel) on INSERT ... SELECT is rejected"
run "filter=x%3E0" "INSERT INTO t_dst SELECT x FROM t_src"

echo "-- nothing was inserted by any rejected statement"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dst"

echo "-- without construction parameters, INSERT ... SELECT inserts all rows"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-urlencode "query=INSERT INTO t_dst SELECT x FROM t_src"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dst"
