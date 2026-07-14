#!/usr/bin/env bash

# Tests that the SELECT access check added for EXPLAIN QUERY TREE / EXPLAIN SYNTAX
# (https://github.com/ClickHouse/ClickHouse/issues/78938) also covers the base tables read through a
# regular (SQL SECURITY INVOKER) view that is not inlined (analyzer_inline_views = 0, the default).
# In that case the query tree keeps the view as a single TableNode, so without recursing into the
# view's inner query the check would only enforce SELECT on the view object. Real execution reads the
# base tables in StorageView::readImpl under the invoker's rights, so a user with SELECT on the view
# but not on its base table must be denied EXPLAIN exactly as a plain SELECT through the view is.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"
outsider="outsider_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader}, ${full}, ${outsider};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a');

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${reader}, ${full}, ${outsider};
-- The reader can read the view object but has no direct access to the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${reader};
-- The full user can read both the view and the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local user="$1"
    local inline="$2"
    local label="$3"
    local query="$4"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --analyzer_inline_views "${inline}" --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

echo "-- Reader has SELECT on the view but not the base table: EXPLAIN is denied exactly as the SELECT is"
run "${reader}" 0 "SELECT"              "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 0 "EXPLAIN QUERY TREE"  "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 0 "EXPLAIN SYNTAX"      "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 0 "EXPLAIN QUERY TREE count" "EXPLAIN QUERY TREE SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"
# With inlining the base table is already exposed in the tree, so this path was always denied; check it stays consistent.
run "${reader}" 1 "SELECT (inline)"             "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 1 "EXPLAIN QUERY TREE (inline)" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Full user has SELECT on both the view and the base table: EXPLAIN matches SELECT and is allowed"
run "${full}" 0 "SELECT"             "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" 0 "EXPLAIN QUERY TREE" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" 0 "EXPLAIN SYNTAX"     "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" 0 "EXPLAIN QUERY TREE count" "EXPLAIN QUERY TREE SELECT count() FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Outsider has no access at all: EXPLAIN is denied"
run "${outsider}" 0 "EXPLAIN QUERY TREE" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${outsider}" 0 "EXPLAIN SYNTAX"     "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP USER ${reader}, ${full}, ${outsider};
"
