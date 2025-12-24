#!/bin/bash
# Tags: no-parallel-replicas,

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function run_test()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT secondary_indices_compressed_bytes > 0
        FROM system.parts
        WHERE database = currentDatabase() AND table = 't_text_index_materialization' AND active
        ORDER BY name
    "

    $CLICKHOUSE_CLIENT --enable_analyzer 1 -q "SELECT count() FROM t_text_index_materialization WHERE text LIKE '%v322%'"

    $CLICKHOUSE_CLIENT --enable_analyzer 1  -q "
        SELECT trim(explain) FROM
        (
            EXPLAIN actions = 1 SELECT count() FROM t_text_index_materialization WHERE text LIKE '%v322%'
        )
        WHERE explain ILIKE '%filter column%'
    "

    $CLICKHOUSE_CLIENT --enable_analyzer 1 -q "
        SELECT trim(explain) FROM
        (
            EXPLAIN indexes = 1 SELECT count() FROM t_text_index_materialization WHERE text LIKE '%v322%'
        )
        WHERE explain ILIKE '%Granules%'
    "

    $CLICKHOUSE_CLIENT -q "CHECK TABLE t_text_index_materialization SETTINGS check_query_single_value_result = 1"
}

$CLICKHOUSE_CLIENT -q "
    SET enable_full_text_index = 1;
    DROP TABLE IF EXISTS t_text_index_materialization;

    CREATE TABLE t_text_index_materialization
    (
        id UInt64,
        text String
    )
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, merge_max_block_size = 8192;

    INSERT INTO t_text_index_materialization SELECT number, 'v' || toString(number) FROM numbers(100000);

    ALTER TABLE t_text_index_materialization ADD INDEX idx_text (text) TYPE text(tokenizer = ngrams(3));

    INSERT INTO t_text_index_materialization SELECT number, 'v' || toString(number + 1000000) FROM numbers(100000);
"

echo "Before OPTIMIZE FINAL"
run_test

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_text_index_materialization FINAL"
echo "After OPTIMIZE FINAL"
run_test

$CLICKHOUSE_CLIENT --mutations_sync 2 --enable_full_text_index 1 -q "ALTER TABLE t_text_index_materialization CLEAR INDEX idx_text"
echo "After CLEAR INDEX idx_text"
run_test

$CLICKHOUSE_CLIENT --mutations_sync 2 -q "ALTER TABLE t_text_index_materialization MATERIALIZE INDEX idx_text"
echo "After MATERIALIZIZE INDEX idx_text"
run_test

$CLICKHOUSE_CLIENT -q "DROP TABLE t_text_index_materialization"
