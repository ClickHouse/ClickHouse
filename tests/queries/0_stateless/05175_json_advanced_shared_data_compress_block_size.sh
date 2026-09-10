#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/118874: ADVANCED JSON shared data
# must pack its data streams by min_compress_block_size, not one tiny (~200-byte) block per path/substream.
# Covers top-level paths and nested Array(JSON) values (both land in the same shared-data streams).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

check() # $1 - table name, $2 - JSON-string expression to insert
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

    part=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active ORDER BY name LIMIT 1")

    # Mean uncompressed bytes per compressed block across the shared-data 'data' streams (the bug made this ~200).
    mean=$(for f in "$part"/json.object_shared_data.*.data.bin; do
               $CLICKHOUSE_COMPRESSOR --stat < "$f"
           done | awk '{ n++; s += $2 } END { if (n) print int(s / n); else print 0 }')

    [ "$mean" -ge 4096 ] && echo "$1 block size OK" || echo "$1 blocks too small: mean=$mean"
    $CLICKHOUSE_CLIENT -q "DROP TABLE $1"
}

check t_json_adv_flat "toJSONString(mapFromArrays( \
    arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)), \
    arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8))))"

check t_json_adv_nested "toJSONString(map('items', [mapFromArrays( \
    arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)), \
    arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8)))]))"
