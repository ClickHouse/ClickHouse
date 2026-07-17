#!/usr/bin/env bash
# Tags: no-parallel-replicas

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

function run_query_no_idx()
{
    local query=$1
    echo "$query"
    $MY_CLICKHOUSE_CLIENT --use_skip_indexes_on_data_read=0 --query "$query"
}

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS tab;

    CREATE TABLE tab
    (
        id UInt32,
        data JSON,
        INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = array)
    )
    ENGINE = MergeTree
    ORDER BY (id) SETTINGS index_granularity = 1;
"

cat <<'JSON' | $MY_CLICKHOUSE_CLIENT --query "INSERT INTO tab FORMAT JSONEachRow"
{"id":0,"data":{"title":"[\"foo\",\"bar\"]","first":"foo","second":"bar","name":"alice"}}
{"id":1,"data":{"title":"[\"foo\"]","name":"bob"}}
{"id":2,"data":{"title":"[\"bar\"]","name":"carol"}}
{"id":3,"data":{"title":"[\"baz\"]","other":{"first":"foo","second":"bar"},"name":"distractor"}}
JSON

$MY_CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES tab;"

echo "-- Equality on JSON subcolumn"
run_query "SELECT id FROM tab WHERE data.title::String = '[\"foo\",\"bar\"]' ORDER BY id"

echo "-- Equality without index"
run_query_no_idx "SELECT id FROM tab WHERE data.title::String = '[\"foo\",\"bar\"]' ORDER BY id"

echo "-- JSONAllValues hasAllTokens"
run_query "SELECT id FROM tab WHERE hasAllTokens(JSONAllValues(data), ['foo', 'bar']) ORDER BY id"

echo "-- JSONAllValues hasAllTokens without index"
run_query_no_idx "SELECT id FROM tab WHERE hasAllTokens(JSONAllValues(data), ['foo', 'bar']) ORDER BY id"

echo "-- JSONAllValues hasAnyTokens"
run_query "SELECT id FROM tab WHERE hasAnyTokens(JSONAllValues(data), ['foo', 'bar']) ORDER BY id"

echo "-- JSONAllValues hasAnyTokens without index"
run_query_no_idx "SELECT id FROM tab WHERE hasAnyTokens(JSONAllValues(data), ['foo', 'bar']) ORDER BY id"

$MY_CLICKHOUSE_CLIENT --query "DROP TABLE tab;"
