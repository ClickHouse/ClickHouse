#!/usr/bin/env bash

# `EXPLAIN AST optimize = 1` runs the same rewriting visitor as `EXPLAIN SYNTAX`, so it dumps the same
# rewritten query - including the inner query of a parameterized view the call gets inlined into. That
# inlining used to happen through `StorageView::replaceWithSubquery` inside the visitor, which runs no
# base-table access check at all, so a user with `SELECT` on the view but not on its base table could
# read the view definition through `EXPLAIN AST optimize = 1` while a real `SELECT` through the view is
# denied in `StorageView::readImpl`. The parameterized view is now expanded the same way `EXPLAIN SYNTAX`
# expands it, before the visitor runs, so analysing the expanded body checks the base tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

outsider="outsider_${CLICKHOUSE_DATABASE}"
view_only="view_only_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${outsider}, ${view_only}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (x Int32, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 'a');

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.pv AS SELECT x, y FROM ${CLICKHOUSE_DATABASE}.secret_base WHERE x = {n:Int32};

CREATE USER ${outsider}, ${view_only}, ${full};
-- The outsider has no grants at all, the view_only user may read the view but not its base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv TO ${view_only};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
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
    run "${outsider}"  "SELECT             outsider " "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${view_only}" "SELECT             view_only" "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "SELECT             full     " "SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    run "${outsider}"  "EXPLAIN AST        outsider " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${view_only}" "EXPLAIN AST        view_only" "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "EXPLAIN AST        full     " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    # `optimize = 0` (the default) never rewrites the query, so it only ever dumps the user's own text.
    run "${view_only}" "EXPLAIN AST plain  view_only" "EXPLAIN AST SELECT * FROM ${CLICKHOUSE_DATABASE}.pv(n = 1)"

    # An unresolvable query cannot be access-checked, so the dump must fall back to the unexpanded query.
    run "${outsider}"  "EXPLAIN AST unres. outsider " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.nonexistent_table, ${CLICKHOUSE_DATABASE}.pv(n = 1)"
    run "${full}"      "EXPLAIN AST unres. full     " "EXPLAIN AST optimize = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.nonexistent_table, ${CLICKHOUSE_DATABASE}.pv(n = 1)"
done

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.pv;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${outsider}, ${view_only}, ${full};
"
