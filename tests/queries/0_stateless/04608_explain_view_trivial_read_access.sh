#!/usr/bin/env bash

# Tests the trivial-read (e.g. `SELECT count()` / `SELECT 1`) case of the base-table access check for
# non-inlined views added for EXPLAIN QUERY TREE / EXPLAIN SYNTAX
# (https://github.com/ClickHouse/ClickHouse/issues/78938). A trivial read does not resolve the whole view
# body: the planner picks one cheapest readable view column (chooseSmallestColumnToReadFromStorage) and
# StorageView::readImpl passes only that column into the inner query. So `SELECT count() FROM v`, with the
# view defined as `SELECT y, z FROM base`, reads only the cheapest column (`y`, UInt8) from the base table
# and needs base-table access to `y` alone - not to `z`. The EXPLAIN access check must reproduce that
# choice, or it would over-deny: EXPLAIN would reject a trivial query that a plain SELECT allows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

partial="partial_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${partial};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y UInt8, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a');

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${partial};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${partial};
-- Column-level grant on the base: only 'y', not 'z'. 'y' is the cheapest column, so a trivial read
-- through the view reads it and stays within this grant.
GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.base TO ${partial};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local inline="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${partial}" --enable_analyzer 1 --analyzer_inline_views "${inline}" --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

run_legacy() {
    local label="$1"
    local query="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${partial}" --enable_analyzer 0 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

echo "-- Trivial read 'SELECT count()': allowed, matching a plain SELECT (reads only the cheapest column 'y')"
run 0 "SELECT count()"                     "SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN QUERY TREE count()"         "EXPLAIN QUERY TREE SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN SYNTAX count()"             "EXPLAIN SYNTAX SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run_legacy "EXPLAIN SYNTAX count() (legacy)" "EXPLAIN SYNTAX SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Trivial read 'SELECT 1': allowed, matching a plain SELECT"
run 0 "SELECT 1"                           "SELECT 1 FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN QUERY TREE 1"               "EXPLAIN QUERY TREE SELECT 1 FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN SYNTAX 1"                   "EXPLAIN SYNTAX SELECT 1 FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- With inlining the base table is exposed directly; a trivial read still needs only one accessible column"
run 1 "SELECT count() (inline)"            "SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 1 "EXPLAIN QUERY TREE count() (inline)" "EXPLAIN QUERY TREE SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Sanity: explicitly selecting the non-granted column 'z' is still denied, matching a plain SELECT"
run 0 "SELECT z"                           "SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN QUERY TREE z"               "EXPLAIN QUERY TREE SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP USER ${partial};
"
