#!/usr/bin/env bash
# Tags: no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --explain_query_plan_default=legacy"
MY_CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer 1"

function run_query()
{
    local query=$1
    echo "$query"
    # Pick the text index record out of the structured plan by its own name, so that an
    # unrelated index stat (a MinMax stat, for instance, which some settings add) cannot
    # shift the assertion. 'Initial Parts'/'Initial Granules' are the preceding stat's
    # counters, which is what the 'selected/initial' text form prints.
    $MY_CLICKHOUSE_CLIENT --query "
        $query;
        WITH
            assumeNotNull((SELECT explain FROM (EXPLAIN indexes = 1, json = 1 $query))) AS plan_json,
            extract(plan_json, '(\{[^{}]*\"Name\": \"json_idx\".*?\n *\})') AS idx
        SELECT arrayJoin([
            'Description: ' || JSONExtractString(idx, 'Description'),
            'Condition: '   || JSONExtractString(idx, 'Condition'),
            'Parts: '       || toString(JSONExtractUInt(idx, 'Selected Parts'))    || '/' || toString(JSONExtractUInt(idx, 'Initial Parts')),
            'Granules: '    || toString(JSONExtractUInt(idx, 'Selected Granules')) || '/' || toString(JSONExtractUInt(idx, 'Initial Granules'))
        ])
        WHERE throwIf(idx = '', 'text index json_idx not found in the plan') = 0;
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
