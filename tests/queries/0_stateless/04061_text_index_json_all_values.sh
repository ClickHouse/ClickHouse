#!/usr/bin/env bash
# Tags: no-parallel-replicas

# Tests that text indexes built on JSONAllValues can be used with JSON subcolumn queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MY_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer 1"

function run_query()
{
    local query=$1
    echo "$query"
    $MY_CLICKHOUSE_CLIENT --query "$query"

    $MY_CLICKHOUSE_CLIENT --query "
        SELECT trimLeft(explain) FROM (
            EXPLAIN indexes = 1 $query
        )
        WHERE explain LIKE '%Condition:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
        LIMIT 3, 4;
    "
}

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        id UInt32,
        data JSON,
        INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
    )
    ENGINE = MergeTree
    ORDER BY (id) SETTINGS index_granularity = 1;

    INSERT INTO tab VALUES (0, '{\"key1\": \"the quick brown fox\", \"key2\": \"hello world\"}');
    INSERT INTO tab VALUES (1, '{\"key1\": \"lazy dog jumps\", \"key2\": \"goodbye world\"}');
    INSERT INTO tab VALUES (2, '{\"key1\": \"quick silver\", \"num\": 42}');
    INSERT INTO tab VALUES (3, '{\"key1\": \"nothing special\", \"num\": 100}');
"

echo "-- Direct subcolumn access"
run_query "SELECT id FROM tab WHERE data.key1 = 'the quick brown fox' ORDER BY id"
run_query "SELECT id FROM tab WHERE data.num = 42 ORDER BY id"
run_query "SELECT id FROM tab WHERE data.key1 LIKE '%quick%' ORDER BY id"
run_query "SELECT id FROM tab WHERE startsWith(data.key1, 'lazy') ORDER BY id"
run_query "SELECT id FROM tab WHERE endsWith(data.key1, 'fox') ORDER BY id"
run_query "SELECT id FROM tab WHERE hasToken(data.key1, 'quick') ORDER BY id"

echo "-- CAST with ::String"
run_query "SELECT id FROM tab WHERE hasAllTokens(data.key1::String, 'the quick brown fox') ORDER BY id"
run_query "SELECT id FROM tab WHERE data.key1::String = 'the quick brown fox' ORDER BY id"
run_query "SELECT id FROM tab WHERE data.key1::String LIKE '%quick%' ORDER BY id"
run_query "SELECT id FROM tab WHERE startsWith(data.key1::String, 'lazy') ORDER BY id"
run_query "SELECT id FROM tab WHERE hasToken(data.key1::String, 'quick') ORDER BY id"
run_query "SELECT id FROM tab WHERE hasAnyTokens(data.key1::String, ['quick', 'lazy']) ORDER BY id"
run_query "SELECT id FROM tab WHERE hasAllTokens(data.key1::String, ['the', 'quick']) ORDER BY id"

echo "-- IN operator"
run_query "SELECT id FROM tab WHERE data.key1::String IN ('the quick brown fox', 'lazy dog jumps') ORDER BY id"

echo "-- JSON values that are arrays"

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE tab;

    CREATE TABLE tab
    (
        id UInt32,
        data JSON,
        INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
    )
    ENGINE = MergeTree
    ORDER BY (id) SETTINGS index_granularity = 1;

    INSERT INTO tab VALUES (0, '{\"tags\": [\"foo\", \"bar\"], \"name\": \"alice\"}');
    INSERT INTO tab VALUES (1, '{\"tags\": [\"baz\", \"qux\"], \"name\": \"bob\"}');
    INSERT INTO tab VALUES (2, '{\"tags\": \"not_an_array\", \"name\": \"carol\"}');
"

run_query "SELECT id FROM tab WHERE data.tags = ['foo', 'bar'] ORDER BY id"
run_query "SELECT id FROM tab WHERE data.tags = 'not_an_array' ORDER BY id"
run_query "SELECT id FROM tab WHERE hasAllTokens(data.tags::String, 'foo') ORDER BY id"
run_query "SELECT id FROM tab WHERE hasAllTokens(data.tags::String, 'not array') ORDER BY id"

echo "-- Nested JSON subcolumns"

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE tab;

    CREATE TABLE tab
    (
        id UInt32,
        data JSON,
        INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = 'splitByNonAlpha')
    )
    ENGINE = MergeTree
    ORDER BY (id) SETTINGS index_granularity = 1;

    INSERT INTO tab VALUES (0, '{\"a\": {\"b\": \"deep value one\"}}');
    INSERT INTO tab VALUES (1, '{\"a\": {\"b\": \"deep value two\"}}');
    INSERT INTO tab VALUES (2, '{\"a\": {\"b\": \"something else\"}}');
"

run_query "SELECT id FROM tab WHERE data.a.b = 'deep value one' ORDER BY id"

$MY_CLICKHOUSE_CLIENT --query "DROP TABLE tab;"
