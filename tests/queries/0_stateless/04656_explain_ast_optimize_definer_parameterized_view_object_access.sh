#!/usr/bin/env bash

# `EXPLAIN AST optimize = 1` inlines a parameterized view call with the old interpreter's rewriting
# visitor. For a view created with `SQL SECURITY DEFINER` (or `NONE`) the explain-time expansion of the
# call is deliberately skipped - re-analysing the body under the invoker's context would deny users who
# may query the view but not its base tables - yet the visitor still inlines `query_info.view_query`
# itself. The `SELECT` grant on the view object must be enforced for those views too: with the analyzer
# a real `SELECT ... FROM pv(...)` is rejected on the view object in
# `prepareBuildQueryPlanForTableExpression`, so a user with a grant on the base table but none on the
# view must not be able to read the view definition through `EXPLAIN AST optimize = 1`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base_only="base_only_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${base_only}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (x Int32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 'a');

CREATE VIEW ${CLICKHOUSE_DATABASE}.pv_definer
    DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE VIEW ${CLICKHOUSE_DATABASE}.pv_none
    SQL SECURITY NONE
    AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${base_only}, ${full};
-- The base_only user may read the base table but has no grant on either view object.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${base_only};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_none TO ${full};
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

    for view in pv_definer pv_none
    do
        # The real read through the view: the reference below pins that EXPLAIN behaves the same way.
        run "${base_only}" "SELECT      ${view} base_only" "SELECT * FROM ${CLICKHOUSE_DATABASE}.${view}(n = 1)"
        run "${full}"      "SELECT      ${view} full     " "SELECT * FROM ${CLICKHOUSE_DATABASE}.${view}(n = 1)"

        run "${base_only}" "EXPLAIN AST ${view} base_only" "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.${view}(n = 1)"
        run "${full}"      "EXPLAIN AST ${view} full     " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.${view}(n = 1)"
    done
done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv_definer;
DROP VIEW ${CLICKHOUSE_DATABASE}.pv_none;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${base_only}, ${full};
"
