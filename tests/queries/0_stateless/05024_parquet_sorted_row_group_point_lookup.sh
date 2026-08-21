#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

sorted=05024_sorted.parquet
unsorted=05024_unsorted.parquet
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

$CLICKHOUSE_CLIENT --query_id=05024_sorted --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2500 $settings"
$CLICKHOUSE_CLIENT --query_id=05024_cached --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2501 $settings"
$CLICKHOUSE_CLIENT --query_id=05024_absent --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=6000 $settings"
$CLICKHOUSE_CLIENT --query_id=05024_unsorted --query "SELECT count() FROM file('$unsorted', Parquet, 'key UInt64, residual UInt64') WHERE key=1005 $settings"
$CLICKHOUSE_CLIENT --query_id=05024_or --query "SELECT count() FROM file('$sorted', Parquet, 'key UInt64, residual UInt64') WHERE key=2500 OR residual=999 $settings"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"

$CLICKHOUSE_CLIENT --query "
    SELECT query_id,
           ProfileEvents['ParquetRowGroupMinMaxPredicateChecks'],
           ProfileEvents['ParquetReadRowGroups'],
           ProfileEvents['ParquetPrunedRowGroups'],
           ProfileEvents['ParquetOrderedRowGroupIndexCacheHits']
    FROM system.query_log
    WHERE type='QueryFinish' AND startsWith(query_id, '05024_')
    ORDER BY query_id"
