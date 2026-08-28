#!/usr/bin/env bash

# Construction settings that arrive via the HTTP request (a `?limit=` / `?offset=` URL parameter, or a
# `?filter=` kept in the HTTP combined-filter channel) shape a query's result. Like an in-query or
# session setting, they apply to a result-producing query but do not propagate into the source `SELECT`
# of a write-producing statement (`INSERT ... SELECT`), so such an INSERT processes all rows. The same
# parameters on a plain SELECT are effective.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_src (x UInt64) ENGINE=Memory AS SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_dst (x UInt64) ENGINE=Memory"

# Send the query as the raw request body (not a `query=` form field): the dynamic handler reads the
# `query` parameter only from the URL, so a `--data-urlencode "query=…"` body is treated as the query
# text verbatim. Sending via the URL with `--get` would turn the request into a GET and force
# `readonly = 2`, which rejects the INSERT for an unrelated reason.
http() {
    local params="$1"
    local query="$2"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&${params}" --data-binary "${query}"
}

echo "-- ?limit= shapes a plain SELECT (2 rows)"
http "limit=2" "SELECT x FROM t_src ORDER BY x"

echo "-- ?limit= on INSERT ... SELECT does not propagate to the SELECT (all 10 rows inserted)"
http "limit=2" "INSERT INTO t_dst SELECT x FROM t_src"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dst"

echo "-- ?filter= (combined-filter channel) on INSERT ... SELECT does not propagate either (all 10 rows)"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t_dst"
http "filter=x%3E100" "INSERT INTO t_dst SELECT x FROM t_src"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_dst"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dst"
