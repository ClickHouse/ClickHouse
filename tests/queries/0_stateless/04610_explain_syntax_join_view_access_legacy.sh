#!/usr/bin/env bash

# Companion of 04603_explain_syntax_invoker_view_access_legacy for a regular SQL SECURITY INVOKER view
# that appears on the JOIN side (not the FROM/leftmost table) of the query, with the old interpreter
# (allow_experimental_analyzer = 0).
#
# The legacy `EXPLAIN SYNTAX` base-table access check used to look only at the leftmost table expression
# (`getTableExpression(select, 0)`): `query_info.view_query` and `StorageView::replaceWithSubquery` handle
# only the first table expression, so a view on the JOIN side never reached the base-table check. A user
# with `SELECT` on the view object but not on its base table could then `EXPLAIN SYNTAX ... JOIN v_invoker`
# and leak the expanded view body, even though the real `SELECT` is rejected in `StorageView::readImpl`.
# The check now iterates every table expression, so the JOIN-side view is denied exactly as the SELECT is.

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

-- 'other' is a plain table the user can read; the view is only ever referenced on the JOIN side.
CREATE TABLE ${CLICKHOUSE_DATABASE}.other (y Int32) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.other VALUES (1);

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${reader}, ${full}, ${outsider};
-- The reader can read 'other' and the view object, but has no direct access to the view's base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${reader};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${reader};
-- The full user can read everything, including the view's base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
-- The outsider can read only 'other'.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${outsider};
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

query="SELECT o.y FROM ${CLICKHOUSE_DATABASE}.other AS o JOIN ${CLICKHOUSE_DATABASE}.v_invoker AS v ON o.y = v.y"

echo "-- Reader has SELECT on the view but not its base table: the JOIN-side view is denied exactly as the SELECT is"
run "${reader}" "SELECT"         "${query}"
run "${reader}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"

echo "-- Full user has SELECT on the view and its base table: EXPLAIN SYNTAX matches SELECT and is allowed"
run "${full}" "SELECT"         "${query}"
run "${full}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"

echo "-- Outsider has no access to the view: EXPLAIN SYNTAX is denied"
run "${outsider}" "SELECT"         "${query}"
run "${outsider}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX ${query}"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP TABLE ${CLICKHOUSE_DATABASE}.other;
DROP USER ${reader}, ${full}, ${outsider};
"
