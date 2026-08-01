#!/usr/bin/env bash

# A parameterized view is resolved into a `TableFunctionNode`, so `collectSelectedColumnsForTableExpression`
# has to treat it as an `ALIAS`-column carrier exactly like the planner's `CollectTableExpressionData`
# does. If it did not, the visitor would descend into an `ALIAS` column's defining expression and collect
# the columns that expression reads, on top of the alias name itself. The planner keeps only the selected
# alias name (`TableExpressionData::getSelectedColumnsNames`), so the access check would then demand grants
# the real `SELECT` never asks for, and `EXPLAIN` would reject a query that plain execution allows.
#
# The base table's `a` here is an `ALIAS` column, and the parameterized view selects it alongside `x`. The
# user is granted the view's `a` but not its `x`, so `EXPLAIN` must stay allowed for `a` alone.
#
# Today `Context::buildParameterizedViewStorage` builds the view's `ColumnsDescription` from the inner
# query's sample block, so the view's own output columns are ordinary rather than `ALIAS`, and the descent
# never happens for the view object itself. This test pins the contract - `EXPLAIN` outcome equals plain
# `SELECT` outcome - so that it keeps holding if that ever changes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="alias_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${user};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (x Int32, a Int32 ALIAS x + 1) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.base (x) VALUES (1);

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT a, x FROM ${CLICKHOUSE_DATABASE}.base WHERE x = {p:Int32};

CREATE USER ${user};
-- Everything the view body itself needs on the base table.
GRANT SELECT(x, a) ON ${CLICKHOUSE_DATABASE}.base TO ${user};
-- On the view object, only the ALIAS-defined output column 'a', not 'x'.
GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.pv TO ${user};
"

# Print 'OK' when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons, and the full
# unexpected error otherwise, so a case that starts throwing something else cannot silently pass.
run() {
    local label="$1"
    local query="$2"
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

echo "-- Selecting only the granted ALIAS column 'a': EXPLAIN must match the plain SELECT"
run "SELECT a"                "SELECT a FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "EXPLAIN QUERY TREE a"    "EXPLAIN QUERY TREE SELECT a FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "EXPLAIN SYNTAX a"        "EXPLAIN SYNTAX SELECT a FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "EXPLAIN AST optimize a"  "EXPLAIN AST optimize = 1 SELECT a FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

echo "-- Selecting the non-granted column 'x': EXPLAIN must match the plain SELECT here too"
run "SELECT x"                "SELECT x FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "EXPLAIN QUERY TREE x"    "EXPLAIN QUERY TREE SELECT x FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"
run "EXPLAIN SYNTAX x"        "EXPLAIN SYNTAX SELECT x FROM ${CLICKHOUSE_DATABASE}.pv(p = 1)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP USER ${user};
"
