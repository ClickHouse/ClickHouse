#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --explain_query_plan_default=legacy"

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS json_values_bloom_negative;
    CREATE TABLE json_values_bloom_negative
    (
        id UInt32,
        data JSON(a Int64, s String),
        INDEX json_idx JSONAllValues(data) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY tuple()
    SETTINGS index_granularity = 2;

    INSERT INTO json_values_bloom_negative VALUES
        (0, '{\"a\": 100, \"s\": \"drop\"}'),
        (1, '{\"a\": 1, \"b\": \"100\", \"s\": \"keep\", \"t\": \"drop\"}');
"

# Both rows are in one index granule. A Bloom-filter hit makes each negative
# predicate unknown, so the granule must be retained for the row-level filter.
$CLICKHOUSE_CLIENT --query "
    SELECT id
    FROM json_values_bloom_negative
    WHERE data.a != 100
    SETTINGS force_data_skipping_indices = 'json_idx'
"
$CLICKHOUSE_CLIENT --query "
    SELECT id
    FROM json_values_bloom_negative
    WHERE data.s NOT LIKE '%drop%'
    SETTINGS force_data_skipping_indices = 'json_idx'
"

$CLICKHOUSE_CLIENT --query "DROP TABLE json_values_bloom_negative"
