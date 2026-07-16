#!/usr/bin/env bash

# Companion of 04601_explain_query_tree_access_invoker_view for the old interpreter
# (allow_experimental_analyzer = 0).
#
# `ExplainAnalyzedSyntaxMatcher` only checked `SELECT` on the view object itself
# (`checkAccessRightsForSelect` inside `InterpreterSelectQuery::analyze`, using the un-rewritten
# `table_id`). A real `SELECT` through a regular (SQL SECURITY INVOKER, non-parameterized) view goes
# on to check the base-table privileges too, but only once the pipeline is actually built, in
# `StorageView::readImpl` - which `EXPLAIN SYNTAX` never reaches. So a user with `SELECT` on the view
# but not on its base table could `EXPLAIN SYNTAX` the expanded view body while the real `SELECT` is
# denied. This must be denied here too, exactly as the analyzer path already is.

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
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 0 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

echo "-- Reader has SELECT on the view but not the base table: EXPLAIN SYNTAX is denied exactly as the SELECT is"
run "${reader}" "SELECT"         "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Full user has SELECT on both the view and the base table: EXPLAIN SYNTAX matches SELECT and is allowed"
run "${full}" "SELECT"         "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Outsider has no access at all: EXPLAIN SYNTAX is denied"
run "${outsider}" "SELECT"         "SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${outsider}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v_invoker"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${reader}, ${full}, ${outsider};
"
