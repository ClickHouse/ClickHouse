#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

sorted=${CLICKHOUSE_TEST_UNIQUE_NAME}_sorted.parquet
unsorted=${CLICKHOUSE_TEST_UNIQUE_NAME}_unsorted.parquet
QID_PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_${$}_"
trap 'rm -f "$USER_FILES_PATH/$sorted" "$USER_FILES_PATH/$unsorted"' EXIT

settings="SETTINGS use_query_condition_cache=0, input_format_parquet_bloom_filter_push_down=0, input_format_parquet_page_filter_push_down=0, input_format_parquet_dictionary_filter_push_down=0, max_threads=1, max_parsing_threads=1"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION file('$sorted', Parquet, 'key UInt64, residual UInt64')
    SELECT number, number % 7 FROM numbers(5090)
    SETTINGS output_format_parquet_row_group_size=10, max_threads=1,
             max_insert_threads=1, max_block_size=5090, engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION file('$unsorted', Parquet, 'key UInt64, residual UInt64')
    SELECT multiIf(number >= 1000 AND number < 1010, number + 10,
                   number >= 1010 AND number < 1020, number - 10, number), number % 7
    FROM numbers(5090)
    SETTINGS output_format_parquet_row_group_size=10, max_threads=1,
             max_insert_threads=1, max_block_size=5090, engine_file_truncate_on_insert=1"

$CLICKHOUSE_CLIENT --query_id="${QID_PREFIX}sorted" --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2500 $settings"
$CLICKHOUSE_CLIENT --query_id="${QID_PREFIX}cached" --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2501 $settings"
$CLICKHOUSE_CLIENT --query_id="${QID_PREFIX}absent" --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=6000 $settings"
$CLICKHOUSE_CLIENT --query_id="${QID_PREFIX}unsorted" --query "SELECT count() FROM file('$unsorted', Parquet, 'key UInt64, residual UInt64') WHERE key=1005 $settings"
$CLICKHOUSE_CLIENT --query_id="${QID_PREFIX}or" --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2500 OR residual=999 $settings"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT concat('05024_', substring(query_id, length('${QID_PREFIX}') + 1)) AS name,
           ProfileEvents['ParquetRowGroupMinMaxPredicateChecks'],
           ProfileEvents['ParquetReadRowGroups'],
           ProfileEvents['ParquetPrunedRowGroups'],
           ProfileEvents['ParquetOrderedRowGroupIndexCacheHits']
    FROM system.query_log
    WHERE type='QueryFinish' AND startsWith(query_id, '${QID_PREFIX}') AND query_id != '${QID_PREFIX}cleared'
    ORDER BY name"

# Clearing the Parquet metadata cache must also drop the fence index: the next
# point lookup rebuilds it (miss, no hit).
$CLICKHOUSE_CLIENT --query "SYSTEM CLEAR PARQUET METADATA CACHE"
$CLICKHOUSE_CLIENT --query_id="${QID_PREFIX}cleared" --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2502 $settings"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['ParquetOrderedRowGroupIndexCacheMisses'],
           ProfileEvents['ParquetOrderedRowGroupIndexCacheHits']
    FROM system.query_log
    WHERE type='QueryFinish' AND query_id='${QID_PREFIX}cleared'"
