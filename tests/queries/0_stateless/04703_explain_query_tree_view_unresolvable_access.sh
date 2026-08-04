#!/usr/bin/env bash

# The analyzer `EXPLAIN QUERY TREE` / `EXPLAIN SYNTAX run_query_tree_passes = 1` paths reproduce the
# base-table `SELECT` check `StorageView::readImpl` would run for a non-inlined view by resolving the
# view's inner query with the analyzer (`checkViewBaseTableAccess`). That check skips itself when the
# inner query is analyzer-unresolvable (e.g. `GROUP BY GROUPING SETS (...) WITH TOTALS` from a view
# created under `enable_analyzer = 0`, which the analyzer rejects with `NOT_IMPLEMENTED`). The skip must
# be propagated: dumping the resolved outer tree then would hand out resolved metadata after only the
# view-object grant, even though the base-table access pass never ran. `EXPLAIN` falls back to dumping
# the unresolved tree (the user's own query text, `*` not expanded into column names) in that case,
# matching the fail-close the legacy `EXPLAIN SYNTAX` formatter implements by keeping such a view
# unexpanded. A real `SELECT` through such a view fails while resolving the inner query, so no
# successfully running query loses its resolved `EXPLAIN`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

outsider="outsider_${CLICKHOUSE_DATABASE}"
view_only="view_only_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${outsider}, ${view_only}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.secret_base (fact_id Int32, other_id Int32, sales_value Int32) ENGINE = MergeTree ORDER BY fact_id;
INSERT INTO ${CLICKHOUSE_DATABASE}.secret_base VALUES (1, 2, 3);
"

# Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges. The
# inner query uses GROUPING SETS ... WITH TOTALS, which the analyzer resolver rejects with
# NOT_IMPLEMENTED, so the base-table access check cannot run. The analyzer also rejects it at CREATE VIEW
# time, so the view is created with the old interpreter (enable_analyzer = 0).
${CLICKHOUSE_CLIENT} --enable_analyzer 0 --query "
CREATE VIEW ${CLICKHOUSE_DATABASE}.v AS
    SELECT fact_id, other_id, sum(sales_value) AS sales_value
    FROM ${CLICKHOUSE_DATABASE}.secret_base
    GROUP BY GROUPING SETS (fact_id, (fact_id, other_id)) WITH TOTALS
"

${CLICKHOUSE_CLIENT} --query "
CREATE USER ${outsider}, ${view_only}, ${full};
-- The outsider has no grants. The view_only user can read the view but not its base table. The full user
-- can read both.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v TO ${view_only};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${full};
"

# Reports whether the query succeeded and whether its output dumped resolved metadata. The resolved tree
# expands `*` into the view's column names and types (`COLUMN ... result_type: ...` nodes / a projected
# column list naming `sales_value`); the unresolved fallback keeps the user's own query text (an ASTERISK
# matcher / `SELECT *`), which mentions neither.
run() {
    local user="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer 1 --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -ne 0 ]; then
        if echo "${out}" | grep -q "ACCESS_DENIED"; then
            echo "${label}: ACCESS_DENIED"
        else
            echo "${label}: UNEXPECTED ERROR: ${out}"
        fi
        return
    fi
    if echo "${out}" | grep -q -E "result_type|sales_value"; then
        echo "${label}: OK, resolved metadata revealed"
    else
        echo "${label}: OK, resolved metadata not revealed"
    fi
}

query_tree="EXPLAIN QUERY TREE SELECT * FROM ${CLICKHOUSE_DATABASE}.v"
syntax="EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.v"

# The outsider is denied on the view object itself. For the view_only user the base-table check cannot
# run (the inner query is analyzer-unresolvable), so the resolved tree must not be dumped - otherwise the
# view's resolved columns and types would be handed out with no base-table check ever having run. The
# full user gets the same fail-safe unresolved fallback.
run "${outsider}"  "query tree, outsider"  "${query_tree}"
run "${view_only}" "query tree, view_only" "${query_tree}"
run "${full}"      "query tree, full"      "${query_tree}"
run "${outsider}"  "syntax, outsider"      "${syntax}"
run "${view_only}" "syntax, view_only"     "${syntax}"
run "${full}"      "syntax, full"          "${syntax}"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${outsider}, ${view_only}, ${full};
"
