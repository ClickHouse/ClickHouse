#!/usr/bin/env bash

# Tests that the SELECT access check added for EXPLAIN QUERY TREE / EXPLAIN SYNTAX
# (https://github.com/ClickHouse/ClickHouse/issues/78938) recurses into the base tables of a
# parameterized view, exactly as a real SELECT through the view does.
#
# A parameterized view call `pv(...)` is turned into a fake TableNode for the view by
# `QueryAnalyzer::resolveTableFunction`; real execution reads its base tables through
# `StorageView::readImpl` under the view's own security context. Without the recursive base-table
# check, a user with SELECT on the view but no grant on its base table could read base-table
# metadata via `EXPLAIN QUERY TREE SELECT * FROM pv(...)` (and via `EXPLAIN SYNTAX` when expansion
# is skipped, e.g. for FINAL) even though the real SELECT is denied. The check must match the real
# SELECT in both directions: deny when the base is read as a user without access (SQL SECURITY
# INVOKER), and allow when the base is read as a privileged definer (SQL SECURITY DEFINER).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

full="full_${CLICKHOUSE_DATABASE}"
view_only="view_only_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${full}, ${view_only};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a'), (2, 'b');

-- Parameterized view, SQL SECURITY INVOKER by default: reads the base table as the invoker.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base WHERE y = {p:Int32};

CREATE USER ${full}, ${view_only};
-- Full access needs both grants because an INVOKER view resolves its body under the invoker.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv   TO ${full};
-- Access to the view object only, not to the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv   TO ${view_only};

-- Parameterized view with SQL SECURITY DEFINER: reads the base table as the (privileged) definer.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv_def DEFINER = ${full} SQL SECURITY DEFINER
    AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base WHERE y = {p:Int32};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_def TO ${view_only};
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

echo "-- INVOKER view, full access (base + view): every form of EXPLAIN matches SELECT and is allowed"
run "${full}"      "SELECT"                  "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${full}"      "EXPLAIN SYNTAX"          "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${full}"      "EXPLAIN QUERY TREE"      "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${full}"      "EXPLAIN QUERY TREE FINAL" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1) FINAL"

echo "-- INVOKER view, access to the view but not the base table: every form of EXPLAIN is denied exactly as SELECT is"
run "${view_only}" "SELECT"                  "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${view_only}" "EXPLAIN SYNTAX"          "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${view_only}" "EXPLAIN QUERY TREE"      "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${view_only}" "EXPLAIN SYNTAX FINAL"    "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1) FINAL"
run "${view_only}" "EXPLAIN QUERY TREE FINAL" "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1) FINAL"

echo "-- DEFINER view, access to the view but not the base table: allowed exactly as SELECT is (base read as the definer)"
run "${view_only}" "SELECT"                  "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv_def(p = 1)"
run "${view_only}" "EXPLAIN SYNTAX"          "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv_def(p = 1)"
run "${view_only}" "EXPLAIN QUERY TREE"      "EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.pv_def(p = 1)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv_def;
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${full}, ${view_only};
"
