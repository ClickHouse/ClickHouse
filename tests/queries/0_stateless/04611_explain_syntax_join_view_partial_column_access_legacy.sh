#!/usr/bin/env bash

# Companion of 04610_explain_syntax_join_view_access_legacy for column-level grants on the base table
# of a JOIN-side view.
#
# The legacy `EXPLAIN SYNTAX` base-table access check used to send every JOIN-side view down the
# trivial-read path (an empty column list), which only requires one cheapest readable view column.
# With column-level grants that is weaker than real execution: a query reading `v.z` through the JOIN
# passed `EXPLAIN SYNTAX` when the user could read only `y`, while the real `SELECT` is rejected in
# `StorageView::readImpl` because it actually reads `z`. The check now derives the same column set real
# execution requests from the JOIN-side view (join keys plus the joined columns the query uses, as
# `buildJoinedPlan` derives them), so `EXPLAIN SYNTAX` denies and allows exactly as the `SELECT` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

partial="partial_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${partial};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z String) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 'a');

-- 'other' is a plain table the user can read; the view is only ever referenced on the JOIN side.
CREATE TABLE ${CLICKHOUSE_DATABASE}.other (y Int32) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.other VALUES (1);

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${partial};
-- The partial user can read 'other', the whole view object, but only column 'y' of the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO ${partial};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${partial};
GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.base TO ${partial};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local analyzer="$1"
    local label="$2"
    local query="$3"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${partial}" --enable_analyzer "${analyzer}" --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

query_z="SELECT v.z FROM ${CLICKHOUSE_DATABASE}.other AS o JOIN ${CLICKHOUSE_DATABASE}.v_invoker AS v ON o.y = v.y"
query_y="SELECT v.y FROM ${CLICKHOUSE_DATABASE}.other AS o JOIN ${CLICKHOUSE_DATABASE}.v_invoker AS v ON o.y = v.y"

echo "-- Reading v.z through the JOIN needs base column 'z', which the user may not read: EXPLAIN SYNTAX is denied exactly as the SELECT is"
run 0 "SELECT (legacy)"           "${query_z}"
run 0 "EXPLAIN SYNTAX (legacy)"   "EXPLAIN SYNTAX ${query_z}"
run 1 "SELECT (analyzer)"         "${query_z}"
run 1 "EXPLAIN SYNTAX (analyzer)" "EXPLAIN SYNTAX ${query_z}"

echo "-- Reading only v.y needs just base column 'y', which is granted: EXPLAIN SYNTAX is allowed exactly as the SELECT is"
run 0 "SELECT (legacy)"           "${query_y}"
run 0 "EXPLAIN SYNTAX (legacy)"   "EXPLAIN SYNTAX ${query_y}"
run 1 "SELECT (analyzer)"         "${query_y}"
run 1 "EXPLAIN SYNTAX (analyzer)" "EXPLAIN SYNTAX ${query_y}"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP TABLE ${CLICKHOUSE_DATABASE}.other;
DROP USER ${partial};
"
