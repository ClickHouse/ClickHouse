#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/118874: ADVANCED JSON shared data must
# size its compressed blocks by min_compress_block_size, not one tiny block per path/substream. Checks the mean
# uncompressed size of the compressed blocks a subcolumn read touches (query_log ProfileEvents, so it works on
# object storage too). Covers Wide and Compact parts, nested Array(JSON) and the per-column override.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Mean uncompressed size of the compressed blocks a read touches (measured on the zero-level insert part).
# $1 table, $2 column definition, $3 min_bytes/rows_for_wide_part, $4 table min_compress_block_size,
# $5 inserted JSON-string expression, $6 subcolumn read expression
block_mean()
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $1"
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $1 ($2) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = $3, min_rows_for_wide_part = $3,
        object_shared_data_serialization_version = 'advanced',
        object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
        object_shared_data_buckets_for_wide_part = 8, object_shared_data_buckets_for_compact_part = 8,
        index_granularity = 8192, min_compress_block_size = $4, max_compress_block_size = 1048576"
    $CLICKHOUSE_CLIENT -q "INSERT INTO $1 SELECT $5 FROM numbers(10000) SETTINGS type_json_skip_duplicated_paths = 1, max_insert_block_size = 100000"
    $CLICKHOUSE_CLIENT -q "SELECT $6 FROM $1 FORMAT Null SETTINGS log_comment = '$1'"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
    SELECT intDiv(ProfileEvents['CompressedReadBufferBytes'], nullIf(ProfileEvents['CompressedReadBufferBlocks'], 0))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '$1' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"
    $CLICKHOUSE_CLIENT -q "DROP TABLE $1"
}

FLAT="toJSONString(mapFromArrays(arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 500), range(8)), arrayMap(i -> toString(cityHash64(number, i, 1) % 500), range(8))))"
NESTED="toJSONString(map('items', [mapFromArrays(arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 500), range(8)), arrayMap(i -> toString(cityHash64(number, i, 1) % 500), range(8)))]))"
COL="json JSON(max_dynamic_paths = 0)"

# Wide: a fragmented read lands ~1-2 KB, the fix keeps blocks at min_compress_block_size scale (tens of KB).
w_flat=$(block_mean flat_wide "$COL" 0 65536 "$FLAT" "sum(length(json.key_5::String))")
[ "$w_flat" -ge 4096 ] && echo "flat_wide OK" || echo "flat_wide FAIL (mean=$w_flat)"
w_nested=$(block_mean nested_wide "$COL" 0 65536 "$NESTED" "sum(length(toString(json.items)))")
[ "$w_nested" -ge 4096 ] && echo "nested_wide OK" || echo "nested_wide FAIL (mean=$w_nested)"

# Compact: a single-path read never lands below a few KB even when fragmented, so compare relative to a
# per-column min_compress_block_size = 1 override, which must reach the shared-data block cuts.
ovr=$(block_mean compact_ovr "json JSON(max_dynamic_paths = 0) SETTINGS (min_compress_block_size = 1)" 1000000000 65536 "$FLAT" "sum(length(json.key_5::String))")
ref=$(block_mean compact_ref "$COL" 1000000000 65536 "$FLAT" "sum(length(json.key_5::String))")
[ "$ref" -ge $((ovr * 4)) ] && echo "compact_override OK" || echo "compact_override FAIL (ovr=$ovr ref=$ref)"

# An explicit per-column min_compress_block_size = 0 must be honored (not treated as inherit): it fragments.
z=$(block_mean explicit_zero "json JSON(max_dynamic_paths = 0) SETTINGS (min_compress_block_size = 0)" 0 65536 "$FLAT" "sum(length(json.key_5::String))")
[ "$z" -lt 4096 ] && echo "explicit_zero OK" || echo "explicit_zero FAIL (mean=$z)"
