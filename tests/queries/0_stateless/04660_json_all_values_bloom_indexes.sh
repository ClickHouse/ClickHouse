#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --explain_query_plan_default=legacy"

function create_table()
{
    local index_type=$1

    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS json_values_bloom;
        CREATE TABLE json_values_bloom
        (
            id UInt32,
            data JSON,
            INDEX json_idx JSONAllValues(data) TYPE ${index_type} GRANULARITY 1
        )
        ENGINE = MergeTree
        ORDER BY tuple()
        SETTINGS index_granularity = 1;

        INSERT INTO json_values_bloom VALUES
            (0, '{\"key1\": \"the quick brown fox\", \"num\": 42, \"tags\": [\"foo\", \"bar\"], \"nested\": {\"value\": \"alpha\"}}');
        INSERT INTO json_values_bloom VALUES
            (1, '{\"key1\": \"lazy dog jumps\", \"num\": 100, \"tags\": [\"baz\", \"qux\"], \"nested\": {\"value\": \"beta\"}}');
        INSERT INTO json_values_bloom VALUES
            (2, '{\"key1\": \"quick silver\", \"num\": 7, \"tags\": [\"one\", \"two\"], \"nested\": {\"value\": \"gamma\"}}');
        INSERT INTO json_values_bloom VALUES
            (3, '{\"other\": \"the quick brown fox\", \"num\": 8, \"tags\": [\"red\", \"blue\"]}');
    "
}

function run_query()
{
    local query=$1

    echo "$query"
    $CLICKHOUSE_CLIENT --query "$query"
    $CLICKHOUSE_CLIENT --query "
        SELECT trimLeft(explain)
        FROM (EXPLAIN indexes = 1 ${query})
        WHERE explain LIKE '%Name: json_idx%'
           OR explain LIKE '%Description:%'
           OR explain LIKE '%Parts:%'
           OR explain LIKE '%Granules:%'
    "
}

function check_index_usage()
{
    local query=$1

    echo "$query"
    $CLICKHOUSE_CLIENT --query "
        SELECT count()
        FROM (EXPLAIN indexes = 1 ${query})
        WHERE explain LIKE '%Name: json_idx%'
    "
}

echo "-- bloom_filter"
create_table "bloom_filter(0.0001)"
run_query "SELECT id FROM json_values_bloom WHERE data.key1 = 'the quick brown fox' ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.num = 42 ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.nested.value = 'alpha' ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.tags::Array(String) = ['foo', 'bar'] ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.num::Int64 IN (42, 100) ORDER BY id"
check_index_usage "SELECT id FROM json_values_bloom WHERE data.missing::Int64 = 0"

echo "-- ngrambf_v1"
create_table "ngrambf_v1(3, 256, 2, 0)"
run_query "SELECT id FROM json_values_bloom WHERE data.key1 = 'the quick brown fox' ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.num = 100 ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.key1 LIKE '%silver%' ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE startsWith(data.nested.value::String, 'alp') ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE has(data.tags::Array(String), 'foo') ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.num::Int64 IN (100, 777) ORDER BY id"
check_index_usage "SELECT id FROM json_values_bloom WHERE data.missing::Int64 = 0"

echo "-- tokenbf_v1"
create_table "tokenbf_v1(256, 2, 0)"
run_query "SELECT id FROM json_values_bloom WHERE data.key1 = 'lazy dog jumps' ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.num = 42 ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE startsWith(data.key1::String, 'lazy') ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE hasToken(data.key1::String, 'quick') ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE hasAny(data.tags::Array(String), ['foo', 'missing']) ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO json_values_bloom VALUES (4, '{\"a\": 1, \"b\": \"100\", \"s\": \"keep\", \"t\": \"drop\"}')"
$CLICKHOUSE_CLIENT --query "SELECT id FROM json_values_bloom WHERE data.a != 100 SETTINGS force_data_skipping_indices = 'json_idx'"
$CLICKHOUSE_CLIENT --query "SELECT id FROM json_values_bloom WHERE data.s NOT LIKE '%drop%' SETTINGS force_data_skipping_indices = 'json_idx'"

echo "-- sparse_grams"
create_table "sparse_grams(3, 100, 256, 2, 0)"
run_query "SELECT id FROM json_values_bloom WHERE data.key1 = 'quick silver' ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.num = 100 ORDER BY id"
run_query "SELECT id FROM json_values_bloom WHERE data.key1 LIKE '%brown%' ORDER BY id"

$CLICKHOUSE_CLIENT --query "DROP TABLE json_values_bloom"
