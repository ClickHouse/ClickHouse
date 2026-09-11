#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/118874: ADVANCED JSON shared data must
# size its compressed blocks by min_compress_block_size, not one tiny block per path/substream. Uses query_log
# ProfileEvents (not the .bin files) so it also works on object storage. Covers top-level and nested Array(JSON),
# Wide and Compact parts, and the per-column min_compress_block_size override.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $1 table, $2 column definition, $3 min_bytes/rows_for_wide_part (0 - Wide, large - Compact),
# $4 inserted JSON-string expression, $5 subcolumn read expression, $6 log_comment, $7 expected ('big'|'small')
check()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $1"
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $1 ($2) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = $3, min_rows_for_wide_part = $3,
        object_shared_data_serialization_version = 'advanced',
        object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
        object_shared_data_buckets_for_wide_part = 8, object_shared_data_buckets_for_compact_part = 8,
        index_granularity = 8192, min_compress_block_size = 65536, max_compress_block_size = 1048576"

    $CLICKHOUSE_CLIENT -q "INSERT INTO $1 SELECT $4 FROM numbers(50000) SETTINGS type_json_skip_duplicated_paths = 1"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $1 FINAL"

    $CLICKHOUSE_CLIENT -q "SELECT $5 FROM $1 FORMAT Null SETTINGS log_comment = '$6'"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

    # 'big': the fix keeps blocks at min_compress_block_size scale (tens of KB). 'small': a per-column
    # min_compress_block_size override must actually shrink them (a regression would keep them big).
    $CLICKHOUSE_CLIENT -q "
    SELECT '$6 ' || if(
        (intDiv(ProfileEvents['CompressedReadBufferBytes'], nullIf(ProfileEvents['CompressedReadBufferBlocks'], 0)) >= 4096) = ('$7' = 'big'),
        'OK', 'FAIL')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '$6' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $1"
}

FLAT="toJSONString(mapFromArrays(arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)), arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8))))"
NESTED="toJSONString(map('items', [mapFromArrays(arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)), arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8)))]))"

check t_json_flat_wide    "json JSON(max_dynamic_paths = 0)" 0          "$FLAT"   "sum(length(json.key_5::String))"   flat_wide    big
check t_json_nested_wide  "json JSON(max_dynamic_paths = 0)" 0          "$NESTED" "sum(length(toString(json.items)))" nested_wide  big
check t_json_flat_compact "json JSON(max_dynamic_paths = 0)" 1000000000 "$FLAT"   "sum(length(json.key_5::String))"   flat_compact big
# Per-column min_compress_block_size override must reach the shared-data block cuts (Compact path).
check t_json_override "json JSON(max_dynamic_paths = 0) SETTINGS (min_compress_block_size = 1)" 1000000000 "$FLAT" "sum(length(json.key_5::String))" override_small small
