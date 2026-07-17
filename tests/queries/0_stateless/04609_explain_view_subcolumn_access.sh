#!/usr/bin/env bash

# Regression for the base-table access check of non-inlined views (added for EXPLAIN QUERY TREE /
# EXPLAIN SYNTAX, https://github.com/ClickHouse/ClickHouse/issues/78938) when the requested view column
# name is a compound / subcolumn identifier such as a Nested subcolumn `n.x`.
#
# The check reproduces `StorageView::readImpl` by wrapping the view body as `SELECT <columns> FROM (<body>)`.
# The projected identifiers must be built the same way `normalizeAndValidateQuery` builds them
# (`createIdentifierFromColumnName`), i.e. parsing `n.x` into a two-part identifier. A plain single-part
# `ASTIdentifier("n.x")` fails to resolve, the resolution error is swallowed, and the base-table access
# check would then be skipped entirely - so `EXPLAIN` could proceed on a view whose real `SELECT` is denied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (n Nested(x Int32, y Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES ([1], [2]);

-- The view exposes the Nested subcolumns 'n.x' and 'n.y', whose names are compound identifiers.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT n.x, n.y FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${reader}, ${full};
-- The reader can read the view object but has no direct access to the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${reader};
-- The full user can read both the view and the base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
"

# Print 'OK' only when the query succeeds, 'ACCESS_DENIED' when it is rejected for access reasons,
# and the full unexpected error otherwise (which makes the reference diff fail) so that a positive
# case that starts throwing a different exception cannot silently pass.
run() {
    local user="$1"
    local analyzer="$2"
    local label="$3"
    local query="$4"
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "${user}" --enable_analyzer "${analyzer}" --query "${query}" 2>&1)
    local status=$?
    if [ "${status}" -eq 0 ]; then
        echo "${label}: OK"
    elif echo "${out}" | grep -q "ACCESS_DENIED"; then
        echo "${label}: ACCESS_DENIED"
    else
        echo "${label}: UNEXPECTED ERROR: ${out}"
    fi
}

echo "-- Reader has SELECT on the view but not the base table: reading a subcolumn is denied exactly as the SELECT is"
run "${reader}" 1 "SELECT n.x"                     "SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 1 "EXPLAIN QUERY TREE n.x"         "EXPLAIN QUERY TREE SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 1 "EXPLAIN SYNTAX n.x"             "EXPLAIN SYNTAX SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${reader}" 0 "EXPLAIN SYNTAX n.x (legacy)"    "EXPLAIN SYNTAX SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"

echo "-- Full user has SELECT on both the view and the base table: reading a subcolumn matches SELECT and is allowed"
run "${full}" 1 "SELECT n.x"                     "SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" 1 "EXPLAIN QUERY TREE n.x"         "EXPLAIN QUERY TREE SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" 1 "EXPLAIN SYNTAX n.x"             "EXPLAIN SYNTAX SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"
run "${full}" 0 "EXPLAIN SYNTAX n.x (legacy)"    "EXPLAIN SYNTAX SELECT \`n.x\` FROM ${CLICKHOUSE_DATABASE}.v_invoker"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${reader}, ${full};
"
