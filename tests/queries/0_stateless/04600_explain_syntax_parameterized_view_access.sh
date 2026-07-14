#!/usr/bin/env bash

# Tests that the SELECT access check added for EXPLAIN SYNTAX
# (https://github.com/ClickHouse/ClickHouse/issues/78938) enforces the SELECT grant on a
# parameterized view object, not only on its base tables.
#
# EXPLAIN SYNTAX inlines a `SQL SECURITY INVOKER` parameterized view call `pv(...)` into its inner
# subquery before the query tree is built, so the view object is no longer present in the dumped tree.
# Real execution turns `pv(...)` into a TableNode for the view and checks SELECT on it, so the access
# check must run on the original (unexpanded) query. A user with SELECT on the base table but not on
# the view must be denied EXPLAIN SYNTAX, exactly as a plain SELECT through the view is denied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

full="full_${CLICKHOUSE_DATABASE}"
base_only="base_only_${CLICKHOUSE_DATABASE}"
view_only="view_only_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${full}, ${base_only}, ${view_only};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a'), (2, 'b');

-- Parameterized view (SQL SECURITY INVOKER by default).
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base WHERE y = {p:Int32};

CREATE USER ${full}, ${base_only}, ${view_only};
-- An INVOKER parameterized view resolves its body under the invoker, so a full SELECT needs both grants.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv   TO ${full};
-- Access to the base table only, not to the view object.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${base_only};
-- Access to the view object only, not to the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv   TO ${view_only};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local user="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

echo "-- Full access (base table + view object): EXPLAIN SYNTAX matches SELECT and is allowed"
run "${full}"      "SELECT via parameterized view"         "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${full}"      "EXPLAIN SYNTAX parameterized view"     "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${full}"      "EXPLAIN QUERY TREE parameterized view" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

echo "-- Access to the base table but not the view: EXPLAIN SYNTAX is denied exactly as SELECT is"
run "${base_only}" "SELECT via parameterized view"         "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${base_only}" "EXPLAIN SYNTAX parameterized view"     "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${base_only}" "EXPLAIN QUERY TREE parameterized view" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

echo "-- Access to the view but not the base table: still denied (INVOKER reads the base as the invoker)"
run "${view_only}" "SELECT via parameterized view"         "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${view_only}" "EXPLAIN SYNTAX parameterized view"     "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${full}, ${base_only}, ${view_only};
"
