#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_NAME}"

rm -rf "$DATA_PATH"
trap 'rm -rf "$DATA_PATH"' EXIT

read_postings=$($CLICKHOUSE_LOCAL --path "$DATA_PATH" --multiquery --query "
    SET enable_full_text_index = 1;
    SET use_text_index_postings_cache = 0;
    SET max_memory_usage = 0;

    CREATE TABLE tab_text_index_scoped_postings_cache
    (
        id UInt64,
        s String,
        INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_codec = 'bitpacking') GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 10, index_granularity_bytes = '10Mi';

    INSERT INTO tab_text_index_scoped_postings_cache
    SELECT
        number,
        concat('v', toString(number % 4 + 1))
    FROM numbers(2000);

    OPTIMIZE TABLE tab_text_index_scoped_postings_cache FINAL;

    SELECT count()
    FROM tab_text_index_scoped_postings_cache
    WHERE hasAllTokens(s, ['v1', 'v2', 'v3', 'v4'])
    SETTINGS
        text_index_max_cardinality_per_token_for_analysis = 500,
        query_plan_direct_read_from_text_index = 1,
        use_skip_indexes_on_data_read = 1
    FORMAT Null;

    SELECT sum(value)
    FROM system.events
    WHERE name = 'TextIndexReadPostings';
")

if [[ "$read_postings" -gt 0 && "$read_postings" -le 16 ]]
then
    echo ok
else
    echo "fail: $read_postings"
fi
