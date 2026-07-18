#!/usr/bin/env bash

# Legacy `EXPLAIN SYNTAX` (enable_analyzer = 0) rewrites the FROM view into its inner query with
# `StorageView::replaceWithSubquery` and prints the expanded body. Before dumping it, the base-table
# access check (`checkViewBaseTableAccess`) resolves the view's inner query with the analyzer to
# reproduce the `SELECT` check `StorageView::readImpl` would run. That check is skipped when the inner
# query is legacy-explainable but analyzer-unresolvable (e.g. `GROUP BY GROUPING SETS (...) WITH TOTALS`,
# which the analyzer rejects with `NOT_IMPLEMENTED`). With the check skipped, expanding the view body
# would leak the hidden base-table name to a user who has `SELECT` on the view object but no grant on its
# base table. `EXPLAIN SYNTAX` now leaves such a view reference unexpanded whenever its base-table access
# check could not run, matching the parameterized-view fallback.

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
# inner query uses GROUPING SETS ... WITH TOTALS, which the legacy interpreter formats but the analyzer
# resolver rejects with NOT_IMPLEMENTED, so the base-table access check cannot run. The analyzer also
# rejects it at CREATE VIEW time, so the view is created with the old interpreter (enable_analyzer = 0).
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

# Reports whether the query succeeded and whether its output revealed the view's inner query (the base
# table name only appears in the dump if the view body was expanded into it).
run() {
    local user="$1"
    local label="$2"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --query "EXPLAIN SYNTAX SELECT * FROM ${CLICKHOUSE_DATABASE}.v SETTINGS enable_analyzer = 0" 2>&1)
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

# The outsider is denied on the view object itself. For the view_only user the base-table check cannot run
# (the inner query is analyzer-unresolvable), so the view body must not be expanded - otherwise the hidden
# base table would leak. The full user gets the same fail-safe unexpanded fallback.
run "${outsider}"  "outsider"
run "${view_only}" "view_only"
run "${full}"      "full"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v;
DROP TABLE ${CLICKHOUSE_DATABASE}.secret_base;
DROP USER ${outsider}, ${view_only}, ${full};
"
