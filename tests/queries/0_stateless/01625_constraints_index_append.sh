#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS index_append_test_test;"

$CLICKHOUSE_CLIENT --query "CREATE TABLE index_append_test_test (i Int64, a UInt32, b UInt64, CONSTRAINT c1 ASSUME i <= 2 * b AND i + 40 > a) ENGINE = MergeTree() ORDER BY i;"
$CLICKHOUSE_CLIENT --query "INSERT INTO index_append_test_test VALUES (1, 10, 1), (2, 20, 2);"

function run_with_settings()
{
    query="$1 SETTINGS convert_query_to_cnf = 1\
        , optimize_using_constraints = 1\
        , optimize_move_to_prewhere = 1\
        , optimize_substitute_columns = 1\
        , optimize_append_index = 1"

    if [[ $query =~ "EXPLAIN QUERY TREE" ]]; then query="${query}, allow_experimental_analyzer = 1"; fi

    $CLICKHOUSE_CLIENT --query="$query"

}

run_with_settings "EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE a = 0"
run_with_settings "EXPLAIN QUERY TREE SELECT i FROM index_append_test_test WHERE a = 0" | grep -Fac "indexHint"
run_with_settings "EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE a < 0"
run_with_settings "EXPLAIN QUERY TREE SELECT i FROM index_append_test_test WHERE a < 0" | grep -Fac "indexHint"
run_with_settings "EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE a >= 0"
run_with_settings "EXPLAIN QUERY TREE SELECT i FROM index_append_test_test WHERE a >= 0" | grep -Fac "indexHint"
run_with_settings "EXPLAIN SYNTAX SELECT i FROM index_append_test_test WHERE 2 * b < 100"
run_with_settings "EXPLAIN QUERY TREE SELECT i FROM index_append_test_test WHERE 2 * b < 100" | grep -Fac "indexHint"

$CLICKHOUSE_CLIENT --query "DROP TABLE index_append_test_test;"
