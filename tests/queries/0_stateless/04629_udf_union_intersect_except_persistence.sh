#!/usr/bin/env bash

# Related: https://github.com/ClickHouse/ClickHouse/pull/107582
# What survives a restart is the definition text on disk, so this test reloads it in a
# fresh process. Two carriers write a normalized AST back out as SQL:
# `normalizeCreateFunctionQuery` plus `UserDefinedSQLObjectsDiskStorage` for a SQL UDF,
# and the table metadata file for a materialized view. An unparenthesized
# INTERSECT/EXCEPT child of a UNION chain rebinds when that text is re-read, so a UDF
# returns different rows than it did when it was created, and a materialized view is
# rejected outright. A short `DETACH`/`ATTACH` re-reads the same metadata file but runs
# only `FunctionNameNormalizer` over it, so the chain stays flat and is accepted; startup
# applies `SelectIntersectExceptQueryVisitor` first, and only then is the view validated.
#
# Each case prints the result in the creating session, the stored SQL, then the result in
# a fresh process. The two results must agree.

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

# The reported failure. `checkAllowedQueries` accepts this definition at DDL time because
# the first UNION branch has no table expression, but rejects the rebound shape
# `(SELECT 0 UNION ALL SELECT x FROM src) EXCEPT SELECT 1` that the lost parentheses
# produce, so without the fix the metadata is unloadable and the server does not start:
# `MATERIALIZED VIEW support query with multiple simple UNION [ALL] only`.
MV_PATH="${WORKDIR}/mv_reload"
mkdir -p "${MV_PATH}"
echo "--- mv_reload"
$CLICKHOUSE_LOCAL --path "${MV_PATH}" -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv (x UInt64) ENGINE = MergeTree ORDER BY x
        AS SELECT 0 AS x UNION ALL (SELECT x FROM src EXCEPT SELECT 1);
    SELECT 'created';
"
$CLICKHOUSE_LOCAL --path "${MV_PATH}" -q "
    SELECT 'reloaded', count() FROM mv;
    SELECT 'stored', extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE name = 'mv';
"

rm -rf "${WORKDIR}"
