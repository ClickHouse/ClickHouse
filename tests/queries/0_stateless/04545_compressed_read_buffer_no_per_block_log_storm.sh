#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# no-random-merge-tree-settings: the test pins tiny compress blocks and a wide part on purpose

# `CompressedReadBufferFromFile` must not emit one TEST-level log line per decompressed block:
# assert the TEST log count stays tiny while many blocks are decompressed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    SET enable_json_type = 1;
    DROP TABLE IF EXISTS t_read_buffer_log;
    CREATE TABLE t_read_buffer_log (json JSON(max_dynamic_paths = 0))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS max_compress_block_size = 128, marks_compress_block_size = 128,
             min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
             replace_long_file_name_to_hash = 1, default_compression_codec = 'LZ4';
    INSERT INTO t_read_buffer_log
    SELECT toJSONString(map(repeat('a' || number, 5000), 42)) FROM numbers(300);
"

query_id="read_buffer_log_${CLICKHOUSE_DATABASE}_$$"

# First read of the part -> the block-size predictor deserializes the whole column structure
# prefix -> many tiny compressed blocks are decompressed. send_logs_level=test makes the
# server evaluate the `CompressedReadBufferFromFile` TEST logs (and hence the ProfileEvent
# LogTest counter) regardless of the server's own log level.
# use_uncompressed_cache=0: with the cache on, reads go through CachedCompressedReadBuffer
# (a different class without the removed logs), which would make the test pass even unfixed.
$CLICKHOUSE_CLIENT --send_logs_level=test --query_id="$query_id" -q "
    SET enable_json_type = 1, use_uncompressed_cache = 0;
    SELECT json.a FROM t_read_buffer_log FORMAT Null;
" 2>/dev/null

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# With the fix, LogTest is a handful (buffer-open + seek lines), while thousands of blocks
# are decompressed. Without the fix, LogTest ~= CompressedReadBufferBlocks (>> 1000).
$CLICKHOUSE_CLIENT -q "
    SELECT
        ProfileEvents['CompressedReadBufferBlocks'] > 1000 AS many_blocks,
        ProfileEvents['LogTest'] < 1000 AS few_test_logs
    FROM system.query_log
    WHERE query_id = '$query_id' AND current_database = currentDatabase()
      AND type = 'QueryFinish' AND read_rows > 0
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_read_buffer_log;"
