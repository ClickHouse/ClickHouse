#!/usr/bin/env bash

# Companion of 04610_explain_syntax_join_view_access_legacy for a regular SQL SECURITY INVOKER view that is
# read from a NESTED subquery - a `FROM (SELECT ... FROM v)`, an `IN (SELECT ... FROM v)` and a scalar
# subquery - with the old interpreter (allow_experimental_analyzer = 0).
#
# `ExplainAnalyzedSyntaxVisitor` stops descending at an `ASTSelectQuery`, so the base-table access check ran
# only for the outermost `SELECT`. The outer `InterpreterSelectQuery::analyze` checks just `SELECT` on the
# view object, while the base-table denial of a real query happens in `StorageView::readImpl`, which
# `EXPLAIN` never reaches - so a user with `SELECT` on the view but not on its base table could
# `EXPLAIN SYNTAX` / `EXPLAIN AST optimize = 1` a query the real `SELECT` rejects. The check now runs for
# every nested `SELECT` that reads from such a view, with the same column-level precision.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_DATABASE}"
partial="partial_${CLICKHOUSE_DATABASE}"
full="full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${reader}, ${partial}, ${full};
CREATE TABLE ${CLICKHOUSE_DATABASE}.base (y Int32, z Int32) ENGINE = MergeTree ORDER BY y;
INSERT INTO ${CLICKHOUSE_DATABASE}.base VALUES (1, 2);

-- Default SQL SECURITY is INVOKER: the base table is read with the querying user's own privileges.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_invoker AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;
-- A SQL SECURITY DEFINER view reads its base table under the definer's privileges, so a user granted
-- SELECT on the view must not be denied - not even when the view is read from a nested subquery.
CREATE VIEW ${CLICKHOUSE_DATABASE}.v_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER
    AS SELECT y, z FROM ${CLICKHOUSE_DATABASE}.base;

CREATE USER ${reader}, ${partial}, ${full};
-- The reader can read the view object, but has no access at all to the view's base table.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${reader};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_definer TO ${reader};
-- The partial user can additionally read one base-table column, so a subquery selecting only that column
-- must be allowed and one selecting the other column must be denied - exactly as for the real SELECT.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${partial};
GRANT SELECT(y) ON ${CLICKHOUSE_DATABASE}.base TO ${partial};
-- The full user can read everything.
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${full};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.base TO ${full};
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

from_subquery="SELECT y FROM (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker)"
in_subquery="SELECT count() FROM numbers(3) WHERE number IN (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker)"
scalar_subquery="SELECT (SELECT max(z) FROM ${CLICKHOUSE_DATABASE}.v_invoker)"

for query_kind in from_subquery in_subquery scalar_subquery; do
    query="${!query_kind}"

    echo "-- ${query_kind}: reader has SELECT on the view but not its base table"
    run "${reader}" "SELECT"                 "${query}"
    run "${reader}" "EXPLAIN SYNTAX"         "EXPLAIN SYNTAX ${query}"
    run "${reader}" "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"

    echo "-- ${query_kind}: full user has SELECT on the view and its base table"
    run "${full}" "SELECT"                 "${query}"
    run "${full}" "EXPLAIN SYNTAX"         "EXPLAIN SYNTAX ${query}"
    run "${full}" "EXPLAIN AST optimize=1" "EXPLAIN AST optimize = 1 ${query}"
done

echo "-- no over-denial: the same reader may read a nested SQL SECURITY DEFINER view"
run "${reader}" "SELECT"         "SELECT y FROM (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_definer)"
run "${reader}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX SELECT y FROM (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_definer)"

echo "-- a view read through a WITH table of the enclosing query is checked as well"
run "${reader}" "SELECT"         "WITH c AS (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker) SELECT y FROM c"
run "${reader}" "EXPLAIN SYNTAX" "EXPLAIN SYNTAX WITH c AS (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker) SELECT y FROM c"

echo "-- column-level precision: the partial user may read base column 'y' but not 'z'"
run "${partial}" "SELECT y"         "SELECT y FROM (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker)"
run "${partial}" "EXPLAIN SYNTAX y" "EXPLAIN SYNTAX SELECT y FROM (SELECT y FROM ${CLICKHOUSE_DATABASE}.v_invoker)"
run "${partial}" "SELECT z"         "SELECT z FROM (SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker)"
run "${partial}" "EXPLAIN SYNTAX z" "EXPLAIN SYNTAX SELECT z FROM (SELECT z FROM ${CLICKHOUSE_DATABASE}.v_invoker)"

${CLICKHOUSE_CLIENT} --query "
DROP VIEW ${CLICKHOUSE_DATABASE}.v_invoker;
DROP VIEW ${CLICKHOUSE_DATABASE}.v_definer;
DROP TABLE ${CLICKHOUSE_DATABASE}.base;
DROP USER ${reader}, ${partial}, ${full};
"
