#!/usr/bin/env bash

# Tests that the base-table access check for non-inlined views (added for EXPLAIN QUERY TREE /
# EXPLAIN SYNTAX, https://github.com/ClickHouse/ClickHouse/issues/78938) only requires access to the
# base-table columns actually needed to answer the outer query - not every column the view happens to
# select. Real execution passes the outer query's requested column names into
# InterpreterSelectQueryAnalyzer / InterpreterSelectWithUnionQuery for the view's inner query (see
# StorageView::readImpl), so `SELECT y FROM v` only needs base-table access to `y`, even though the view
# itself is defined as `SELECT y, z FROM base`. The access check must reproduce that column-level
# restriction, or it would over-deny: EXPLAIN would reject a query that a plain SELECT allows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

partial="partial_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${partial};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a');

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${partial};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${partial};
-- Column-level grant: only 'y', not 'z'.
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

echo "-- Selecting only the granted column 'y': allowed, matching a plain SELECT"
run 0 "SELECT y"                     "SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN QUERY TREE y"         "EXPLAIN QUERY TREE SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN SYNTAX y"             "EXPLAIN SYNTAX SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run_legacy "EXPLAIN SYNTAX y (legacy)" "EXPLAIN SYNTAX SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Selecting the non-granted column 'z': denied, matching a plain SELECT"
run 0 "SELECT z"                     "SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN QUERY TREE z"         "EXPLAIN QUERY TREE SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN SYNTAX z"             "EXPLAIN SYNTAX SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run_legacy "EXPLAIN SYNTAX z (legacy)" "EXPLAIN SYNTAX SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Selecting both columns: denied, matching a plain SELECT"
run 0 "SELECT y, z"                  "SELECT y, z FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 0 "EXPLAIN QUERY TREE y, z"      "EXPLAIN QUERY TREE SELECT y, z FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- With inlining the base table is already exposed directly in the tree; behavior stays the same"
run 1 "SELECT y (inline)"            "SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 1 "EXPLAIN QUERY TREE y (inline)" "EXPLAIN QUERY TREE SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 1 "SELECT z (inline)"            "SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run 1 "EXPLAIN QUERY TREE z (inline)" "EXPLAIN QUERY TREE SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP USER ${partial};
"
