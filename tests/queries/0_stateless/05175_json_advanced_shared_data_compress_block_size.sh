#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/118874: ADVANCED JSON shared data must
# size its compressed blocks by min_compress_block_size, not one tiny block per path/substream. Uses query_log
# ProfileEvents (not the .bin files) so it also works on object storage; covers top-level and nested Array(JSON).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

check() # $1 - table, $2 - inserted JSON-string expression, $3 - subcolumn read expression, $4 - log_comment
{
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $1"
    $CLICKHOUSE_CLIENT -q "
    CREATE TABLE $1 (json JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        object_shared_data_serialization_version = 'advanced',
        object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
        object_shared_data_buckets_for_wide_part = 8,
        index_granularity = 8192, min_compress_block_size = 65536, max_compress_block_size = 1048576"

    $CLICKHOUSE_CLIENT -q "INSERT INTO $1 SELECT $2 FROM numbers(50000) SETTINGS type_json_skip_duplicated_paths = 1"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE $1 FINAL"

    $CLICKHOUSE_CLIENT -q "SELECT $3 FROM $1 FORMAT Null SETTINGS log_comment = '$4'"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

    # Mean uncompressed size of the blocks the read touched: the bug fragmented shared data into ~200-1700 B
    # blocks, the fix keeps them at min_compress_block_size scale (tens of KB).
    $CLICKHOUSE_CLIENT -q "
    SELECT '$1 ' || if(intDiv(ProfileEvents['CompressedReadBufferBytes'], nullIf(ProfileEvents['CompressedReadBufferBlocks'], 0)) >= 4096, 'OK', 'blocks too small')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '$4' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1"

    $CLICKHOUSE_CLIENT -q "DROP TABLE $1"
}

check t_json_adv_flat \
    "toJSONString(mapFromArrays( \
        arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)), \
        arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8))))" \
    "sum(length(json.key_5::String))" flat_05175

check t_json_adv_nested \
    "toJSONString(map('items', [mapFromArrays( \
        arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)), \
        arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8)))]))" \
    "sum(length(toString(json.items)))" nested_05175
