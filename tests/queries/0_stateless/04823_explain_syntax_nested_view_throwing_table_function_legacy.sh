#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Azure table functions are not available in the fast test build.

# A nested subquery that reads from a regular `INVOKER` view is analyzed standalone by
# `checkNestedSelectsViewBaseTableAccess` so the view's base-table access is checked where a real
# query checks it. That standalone analysis also resolves any table function the nested subquery
# mentions, and a remote one may throw a non-`DB::Exception` while doing so (here `paimonAzure`
# with an invalid Base64 account key throws `std::runtime_error`, the same shape as a connection
# failure). Such an exception is not an access denial: the nested `SELECT` must be left unchecked -
# the legacy dump never expands nested views, so nothing beyond the user's own text is printed -
# instead of turning the `EXPLAIN` into an exception the real dump path never raised.
#
# The `_subqueryN` alias number the analysis assigns is not stable across runs, so it is normalized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "CREATE TABLE base_04823 (a UInt64) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT --query "CREATE VIEW v_04823 AS SELECT a FROM base_04823"

QUERY="SELECT a
FROM base_04823
WHERE a IN (
    SELECT v.a
    FROM v_04823 AS v
    JOIN paimonAzure('http://localhost:1/devstoreaccount1', 'cont', 'path', 'devstoreaccount1', 'not_a_base64_key') AS p ON v.a = p.x
)"

echo "-- EXPLAIN SYNTAX dumps despite the failing table function in the nested subquery"
$CLICKHOUSE_CLIENT --enable_analyzer=0 --query "EXPLAIN SYNTAX $QUERY" | sed 's/_subquery[0-9][0-9]*/_subquery/g'

echo "-- EXPLAIN AST optimize = 1 dumps as well"
$CLICKHOUSE_CLIENT --enable_analyzer=0 --query "EXPLAIN AST optimize = 1 $QUERY" | sed 's/_subquery[0-9][0-9]*/_subquery/g'

$CLICKHOUSE_CLIENT --query "DROP VIEW v_04823"
$CLICKHOUSE_CLIENT --query "DROP TABLE base_04823"
