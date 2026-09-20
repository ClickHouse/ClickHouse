#!/usr/bin/env bash

# Tests that EXPLAIN variants check SELECT access on the referenced tables and do not leak
# table metadata (column names and types) to users without privileges.
# https://github.com/ClickHouse/ClickHouse/issues/78938

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${CLICKHOUSE_DATABASE}.x (y Int32, z String) ENGINE = MergeTree ORDER BY y;
CREATE USER ${user};
"

# Print 'ACCESS_DENIED' when the query is rejected, 'OK' when it succeeds.
run() {
    local label="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --query "${query}" 2>&1)
    if echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: OK"
    fi
}

echo "-- No privileges: every EXPLAIN variant and SELECT must be denied"
run "SELECT"             "SELECT * FROM ${CLICKHOUSE_DATABASE}.x"
run "EXPLAIN SYNTAX"     "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.x"
run "EXPLAIN PLAN"       "EXPLAIN PLAN SELECT * FROM ${CLICKHOUSE_DATABASE}.x"
run "EXPLAIN QUERY TREE" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.x"
run "EXPLAIN PIPELINE"   "EXPLAIN PIPELINE SELECT * FROM ${CLICKHOUSE_DATABASE}.x"
run "SUBQUERY IN WHERE"  "EXPLAIN QUERY TREE SELECT 1 WHERE 1 IN (SELECT y FROM ${CLICKHOUSE_DATABASE}.x)"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.x TO ${user}"

echo "-- Full SELECT grant: EXPLAIN QUERY TREE is allowed"
run "EXPLAIN QUERY TREE" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.x"

${CLICKHOUSE_CLIENT} --query "
REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.x FROM ${user};
GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.x TO ${user};
"

echo "-- Column-level grant: allowed for granted columns, denied otherwise (as for a plain SELECT)"
run "EXPLAIN QUERY TREE (y)" "EXPLAIN QUERY TREE SELECT y FROM ${CLICKHOUSE_DATABASE}.x"
run "EXPLAIN QUERY TREE (z)" "EXPLAIN QUERY TREE SELECT z FROM ${CLICKHOUSE_DATABASE}.x"
run "EXPLAIN QUERY TREE (*)" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.x"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
