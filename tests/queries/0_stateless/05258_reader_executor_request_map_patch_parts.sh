#!/usr/bin/env bash
# Tags: no-object-storage, no-distributed-cache, no-encrypted-storage, no-parallel-replicas
# The test finds patch parts by their directory in the logged path, which object storage replaces with a random key.
# The executor falls back to the legacy read path for the distributed cache and for decryption, so nothing is announced.
# Parallel replicas learn their ranges from the coordinator only after the readers exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The distinct range counts of the request maps that the executor receives for the files of patch parts.
function patch_range_counts()
{
    $CLICKHOUSE_CLIENT --send_logs_level=test --use_reader_executor=1 --max_threads=1 --apply_patch_parts=1 \
        --remote_filesystem_read_method=read --local_filesystem_read_method=pread --enable_filesystem_cache=0 \
        -q "$1" 2>&1 >/dev/null | grep -o 'Request map of [^ ]*/patch-[^ ]* .*, range count [0-9]*' | grep -o 'range count [0-9]*' | sort -u
}

# Small compressed blocks, so that the byte ranges of separate mark ranges of the small patch part do not merge.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_patched (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0, min_compress_block_size = 1024, max_compress_block_size = 1024,
        enable_block_number_column = 1, enable_block_offset_column = 1;
    INSERT INTO t_patched SELECT number, number FROM numbers(100000);
    OPTIMIZE TABLE t_patched FINAL;
"
# One patch row for every tenth row, so the patch part has granules over the whole key range.
$CLICKHOUSE_CLIENT --enable_lightweight_update=1 -q "UPDATE t_patched SET v = k + 1 WHERE k % 10 = 0"

echo "two key ranges"
patch_range_counts "SELECT sum(v) FROM t_patched WHERE k < 5000 OR k >= 90000"
echo "full scan"
patch_range_counts "SELECT sum(v) FROM t_patched"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_patched"
