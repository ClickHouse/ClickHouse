#!/usr/bin/env bash

# `EXPLAIN AST optimize = 1` inlines a parameterized view call into its parameter-substituted body.
# Analysing that body checks the base tables, but with the analyzer a real `SELECT ... FROM pv(...)`
# also requires a `SELECT` grant on the parameterized view object itself:
# `QueryAnalyzer::resolveTableFunction` turns the call into a view `TableNode` that
# `prepareBuildQueryPlanForTableExpression` checks. Without that check here, a user who may read the base
# table but has no grant on the view could read the view definition through `EXPLAIN AST optimize = 1`
# while the real `SELECT` is denied.
#
# The check must follow what really running the query would require: `EXPLAIN AST` always rewrites with
# the old interpreter's visitor, but that is an implementation detail and must not make the view object's
# grant unnecessary. With the old interpreter the grant genuinely is not required (a real
# `SELECT ... FROM pv(...)` succeeds with the base-table grants alone), so `EXPLAIN` must not deny there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base_only="base_only_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${base_only}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (x Int32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 'a');

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${base_only}, ${full};
-- The base_only user may read the base table but has no grant on the view object.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${base_only};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv TO ${full};
"

# Reports whether the query succeeded and whether its output revealed the view's inner query
# (the base table name only appears in the dump if the view body was inlined into it).
run() {
    local user="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer "${enable_analyzer}" --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -ne 0 ]; then
        if echo "${out}" | grep -q "ACCESS_DENIED"; then
            echo "${label}: ACCESS_DENIED"
        else
            echo "${label}: UNEXPECTED ERROR: ${out}"
        fi
        return
    fi
    if echo "${out}" | grep -q "secret_base"; then
        echo "${label}: OK, view body revealed"
    else
        echo "${label}: OK, view body not revealed"
    fi
}

for enable_analyzer in 1 0
do
    echo "-- enable_analyzer = ${enable_analyzer}"

    # The real read through the view: the reference below pins that EXPLAIN behaves exactly the same way.
    run "${base_only}" "SELECT      base_only" "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "SELECT      full     " "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    run "${base_only}" "EXPLAIN AST base_only" "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "EXPLAIN AST full     " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    # The explained query's own SETTINGS decide which mode a real query would run in.
    run "${base_only}" "EXPLAIN AST base_only, analyzer forced on " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1) SETTINGS allow_experimental_analyzer = 1"
done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${base_only}, ${full};
"
