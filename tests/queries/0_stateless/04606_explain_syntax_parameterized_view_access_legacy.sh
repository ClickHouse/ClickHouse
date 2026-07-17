#!/usr/bin/env bash

# Companion of 04600_explain_syntax_parameterized_view_access for the old interpreter
# (allow_experimental_analyzer = 0).
#
# The old interpreter never requires a SELECT grant on a parameterized view object:
# `Context::executeTableFunction` resolves `pv(...)` without any access check and
# `InterpreterSelectQuery` skips the table access check for table functions, so a real
# `SELECT ... FROM pv(...)` succeeds with only the base-table grants. EXPLAIN SYNTAX must stay
# faithful to that: it is allowed exactly when the real SELECT is allowed (so, unlike the analyzer
# path, a user with base-table access but no grant on the view is NOT denied), and a user without
# base-table access is denied and learns nothing. If the old interpreter ever starts enforcing the
# view-object grant, this test must be updated together with the EXPLAIN access check
# (see the analyzer-only pre-check in `InterpreterExplainQuery.cpp`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

full="full_${CLICKHOUSE_DATABASE}"
base_only="base_only_${CLICKHOUSE_DATABASE}"
outsider="outsider_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${full}, ${base_only}, ${outsider};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a'), (2, 'b');

-- Parameterized view (SQL SECURITY INVOKER by default).
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base WHERE y = {p:Int32};

CREATE USER ${full}, ${base_only}, ${outsider};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv   TO ${full};
-- Access to the base table only, not to the view object.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${base_only};
-- No grants at all for ${outsider}.
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

echo "-- Full access (base table + view object): SELECT and EXPLAIN SYNTAX are allowed"
run "${full}"      "SELECT via parameterized view"     "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${full}"      "EXPLAIN SYNTAX parameterized view" "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

echo "-- Base table access without a grant on the view: the old interpreter allows the real SELECT, so EXPLAIN SYNTAX is allowed too"
run "${base_only}" "SELECT via parameterized view"     "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${base_only}" "EXPLAIN SYNTAX parameterized view" "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

echo "-- No grants: both the real SELECT and EXPLAIN SYNTAX are denied"
run "${outsider}"  "SELECT via parameterized view"     "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "${outsider}"  "EXPLAIN SYNTAX parameterized view" "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${full}, ${base_only}, ${outsider};
"
