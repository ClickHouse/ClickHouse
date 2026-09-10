#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/118874: ADVANCED JSON shared data
# must pack its data streams by min_compress_block_size, not one tiny (~200-byte) block per path/substream.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_adv_blocks"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_json_adv_blocks (json JSON(max_dynamic_paths = 0)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 8,
    index_granularity = 8192, min_compress_block_size = 65536, max_compress_block_size = 1048576"

$CLICKHOUSE_CLIENT -q "
INSERT INTO t_json_adv_blocks SELECT toJSONString(mapFromArrays(
    arrayMap(i -> 'key_' || toString(cityHash64(number, i) % 1000), range(8)),
    arrayMap(i -> toString(cityHash64(number, i, 1) % 1000), range(8))))
FROM numbers(50000) SETTINGS type_json_skip_duplicated_paths = 1"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_json_adv_blocks FINAL"

part=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_json_adv_blocks' AND active ORDER BY name LIMIT 1")

# Mean uncompressed bytes per compressed block across the shared-data 'data' streams (the bug made this ~200).
mean=$(for f in "$part"/json.object_shared_data.*.data.bin; do
           $CLICKHOUSE_COMPRESSOR --stat < "$f"
       done | awk '{ n++; s += $2 } END { if (n) print int(s / n); else print 0 }')

[ "$mean" -ge 4096 ] && echo "block size OK" || echo "blocks too small: mean=$mean"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_adv_blocks"
