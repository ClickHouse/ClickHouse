#!/usr/bin/env bash

# Checks the aggregations that hand a part's compressed blocks to the device against the CPU on a
# table whose compressed blocks end in the middle of a value: `max_compress_block_size` is the size
# of the buffer a column is compressed from, and it is cut wherever it fills, not at a value's end.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 12289 bytes is one more than a multiple of eight, so a block of any of the column types below
# ends within a value. `v_sparse` is mostly zeros and is written sparse, that is without its
# default values, which its `.bin` file alone cannot tell: a part with such a column must be read as
# rows, not as compressed blocks.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_cut_blocks (k UInt32, v_u64 UInt64, v_i16 Int16, v_f32 Float32, v_f64 Float64, v_sparse UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_compress_block_size = 12289, max_compress_block_size = 12289,
             ratio_of_defaults_for_sparse_serialization = 0.5;

    SYSTEM STOP MERGES gpu_cut_blocks;

    -- The float columns hold multiples of an eighth, so that their sums are exact in a double
    -- whatever order the values are added in.
    INSERT INTO gpu_cut_blocks
    SELECT number % 1000, number * 100000000, number % 60000 - 30000, number / 8, number / 8, if(number % 10 = 0, number, 0)
    FROM numbers(0, 1000000);

    INSERT INTO gpu_cut_blocks
    SELECT number % 1000, number * 100000000, number % 60000 - 30000, number / 8, number / 8, if(number % 10 = 0, number, 0)
    FROM numbers(1000000, 300003);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_u64) FROM gpu_cut_blocks" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1)
    elif [[ "$PROBE_ERROR" != *"Cannot aggregate on a GPU"* ]]; then
        echo "$PROBE_ERROR"
    fi
fi

function compare_with_cpu()
{
    local query="$1"
    local on_cpu
    local on_gpu

    on_cpu=$($CLICKHOUSE_CLIENT --query "$query")
    on_gpu=$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "$query")

    if [ "$on_cpu" == "$on_gpu" ]; then
        echo "ok"
    else
        echo "MISMATCH for '$query'"
    fi
}

# Reduced on the device a part at a time, from its compressed blocks. Only sums that keep their
# argument's type are read that way, so the narrow columns come in through minimums and maximums.
compare_with_cpu "SELECT sum(v_u64), sum(v_f64), min(v_i16), max(v_f32) FROM gpu_cut_blocks"
compare_with_cpu "SELECT min(v_u64), max(v_i16), min(v_f32), max(v_f64) FROM gpu_cut_blocks"

# Batches small enough that a batch, too, ends within a value, and its first bytes wait for the
# next batch on the device.
compare_with_cpu "SELECT sum(v_u64), sum(v_f64), max(v_i16) FROM gpu_cut_blocks SETTINGS gpu_aggregation_batch_bytes = 65536"
compare_with_cpu "SELECT max(v_u64), min(v_i16), max(v_f32) FROM gpu_cut_blocks SETTINGS gpu_aggregation_batch_bytes = 4096"

# Grouped on the device from the compressed blocks of every column at once.
compare_with_cpu "SELECT k, sum(v_u64), min(v_i16), max(v_f32), sum(v_f64) FROM gpu_cut_blocks GROUP BY k ORDER BY k"
compare_with_cpu "SELECT k, sum(v_u64), max(v_i16) FROM gpu_cut_blocks GROUP BY k ORDER BY k SETTINGS gpu_aggregation_batch_bytes = 65536"
compare_with_cpu "SELECT k, min(v_f32), sum(v_f64) FROM gpu_cut_blocks GROUP BY k ORDER BY k SETTINGS gpu_aggregation_batch_bytes = 4096"

# The sparse column, whose parts are aggregated from rows instead.
compare_with_cpu "SELECT sum(v_sparse), sum(v_u64) FROM gpu_cut_blocks"
compare_with_cpu "SELECT k, sum(v_sparse), min(v_i16) FROM gpu_cut_blocks GROUP BY k ORDER BY k"
if $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN SELECT sum(v_sparse) FROM gpu_cut_blocks" | grep -q "ReadFromGPUCompressedColumns"; then
    echo "a sparse column was read as compressed blocks"
else
    echo "a sparse column was read as rows"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_cut_blocks"
