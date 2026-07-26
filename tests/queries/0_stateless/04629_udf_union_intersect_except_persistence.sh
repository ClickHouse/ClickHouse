#!/usr/bin/env bash

# Related: https://github.com/ClickHouse/ClickHouse/pull/107582
# SQL UDF bodies are a second carrier of the same formatter bug as
# 04628_union_intersect_except_child_format_parens: normalizeCreateFunctionQuery runs
# SelectIntersectExceptQueryVisitor over the body, and UserDefinedSQLObjectsDiskStorage
# writes that normalized AST back out as SQL. An unparenthesized INTERSECT/EXCEPT child
# of a UNION chain therefore rebinds when the file is re-read, so the function returns
# different rows after a restart than it did when it was created.
#
# Each case prints the result in the creating session, the stored SQL, then the result in
# a fresh process that loads the function from disk. The two results must agree.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A private --path per case: SQL UDFs are server-global, and this keeps the test
# runnable in parallel with itself.
WORKDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_udf_parens"
rm -rf "${WORKDIR}"

run_case()
{
    local case_name="$1"
    local body="$2"
    local path="${WORKDIR}/${case_name}"
    mkdir -p "${path}"

    echo "--- ${case_name}"

    $CLICKHOUSE_LOCAL --path "${path}" -q "
        CREATE FUNCTION f AS x -> (${body});
        SELECT 'created', f(0);
    "

    # The on-disk text is what a restart re-parses. Collapse the pretty-printed layout so
    # the assertion tracks the parentheses only.
    echo -n "stored	"
    $CLICKHOUSE_FORMAT --oneline < "${path}"/user_defined/function_f.sql

    # A fresh process: the function comes back from the file above, not from memory.
    $CLICKHOUSE_LOCAL --path "${path}" -q "SELECT 'reloaded', f(0);"
}

# {1} UNION ALL ({2} EXCEPT {1}) sums to 3. Without the fix the stored SQL loses the
# grouping, re-parses as ({1} UNION ALL {2}) EXCEPT {1} and sums to 2 instead.
run_case except_right "SELECT sum(s) FROM (SELECT 1 AS s UNION ALL (SELECT 2 AS s EXCEPT SELECT 1 AS s))"

# INTERSECT binds tighter than UNION, so the rows survive the lost parentheses here and
# only the stored SQL catches the regression.
run_case intersect_right "SELECT sum(s) FROM (SELECT 1 AS s UNION ALL (SELECT 2 AS s INTERSECT SELECT 2 AS s))"

# The group on the left of the chain.
run_case except_left "SELECT sum(s) FROM ((SELECT 2 AS s EXCEPT SELECT 1 AS s) UNION ALL SELECT 1 AS s)"

# A lone INTERSECT/EXCEPT body has no sibling to rebind to, so it must stay unwrapped:
# this pins the list_of_selects->children.size() > 1 guard.
run_case except_alone "SELECT sum(s) FROM (SELECT 2 AS s EXCEPT SELECT 1 AS s)"

rm -rf "${WORKDIR}"
