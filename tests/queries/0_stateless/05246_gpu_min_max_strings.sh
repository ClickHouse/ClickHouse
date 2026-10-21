#!/usr/bin/env bash

# Checks a `min` and a `max` of `String` columns on the device against the CPU, without keys and grouped, over the
# blocks the pipeline reads and over compressed blocks, with the strings expanded on the device and on the host.
# Expanded on the device, both streams of a column - its bytes and the sizes of its rows - go there compressed, and
# the device turns the sizes into offsets; the blocks are small and of an odd size, so that the two streams end in
# the middle of each other's rows and a block ends in the middle of a size. The strings are empty, short, longer
# than a block and hold zero bytes and bytes above 127, which sort after the ASCII ones by their bytes.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied and both sides of
# every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_min_max_strings (s String, t String, k UInt16, v UInt64, b UInt8)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
        serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
        min_compress_block_size = 12289, max_compress_block_size = 12289;

    SYSTEM STOP MERGES gpu_min_max_strings;
"

for part in 0 1 2; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO gpu_min_max_strings
        SELECT
            multiIf(
                number % 11 = 0, '',
                number % 97 = 1, repeat('x', 20000 + number % 7),
                number % 11 = 2, 'zz' || char(255) || toString(number),
                number % 11 = 3, 'zero' || char(0) || toString(number % 5),
                'key_' || toString(number % 1009)),
            toString(cityHash64(number % 37)),
            number % 13,
            cityHash64(number),
            number % 256
        FROM numbers($part * 300000, 300000 + $part)"
done

# A part of empty strings only, whose bytes expand to nothing.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO gpu_min_max_strings
    SELECT '', '', number % 13, number, number % 256
    FROM numbers(50000)"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v) FROM gpu_min_max_strings" 2>&1 > /dev/null)
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

# Says which step the device aggregated in, or that it did not, as `EXPLAIN` names them.
function device_step()
{
    local query="$1"

    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "$2"
        return
    fi

    local plan
    plan=$($CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN $query")
    if [[ "$plan" == *"ReadFromGPUCompressedColumns"* ]]; then
        echo "compressed"
    elif [[ "$plan" == *"GPUAggregating"* ]]; then
        echo "blocks"
    else
        echo "cpu"
    fi
}

echo "- blocks"

# A filter between the aggregation and the read keeps the device to the blocks the pipeline reads.
WHERE="WHERE b < 200"
SETTINGS="SETTINGS optimize_move_to_prewhere = 0"
device_step "SELECT min(s), max(s) FROM gpu_min_max_strings $WHERE $SETTINGS" blocks
compare_with_cpu "SELECT min(s), max(s), min(t), max(t) FROM gpu_min_max_strings $WHERE $SETTINGS"
compare_with_cpu "SELECT min(s), max(s), sum(v), min(k) FROM gpu_min_max_strings $WHERE $SETTINGS"
compare_with_cpu "SELECT k, min(s), max(t) FROM gpu_min_max_strings $WHERE GROUP BY k ORDER BY k $SETTINGS"
compare_with_cpu "SELECT t, min(s), max(s), sum(v) FROM gpu_min_max_strings $WHERE GROUP BY t ORDER BY t $SETTINGS"
# Batches small enough that the results of many of them are combined.
compare_with_cpu "SELECT min(s), max(s) FROM gpu_min_max_strings $WHERE $SETTINGS, gpu_aggregation_batch_bytes = 4096"
compare_with_cpu "SELECT k, max(s), min(t) FROM gpu_min_max_strings $WHERE GROUP BY k ORDER BY k $SETTINGS, gpu_aggregation_batch_bytes = 65536"
# Reading nothing, so that the result is the empty string.
compare_with_cpu "SELECT min(s), max(s) FROM gpu_min_max_strings WHERE b > 255 $SETTINGS"

echo "- compressed"

device_step "SELECT min(s), max(t) FROM gpu_min_max_strings" compressed
device_step "SELECT k, min(s) FROM gpu_min_max_strings GROUP BY k" compressed
for mode in device host; do
    SETTINGS="SETTINGS gpu_aggregation_decompression = '$mode'"
    compare_with_cpu "SELECT min(s), max(t) FROM gpu_min_max_strings $SETTINGS"
    compare_with_cpu "SELECT max(s), min(t), sum(v), max(k) FROM gpu_min_max_strings $SETTINGS"
    compare_with_cpu "SELECT k, min(s), max(t) FROM gpu_min_max_strings GROUP BY k ORDER BY k $SETTINGS"
    compare_with_cpu "SELECT t, min(s), sum(v) FROM gpu_min_max_strings GROUP BY t ORDER BY t $SETTINGS, gpu_aggregation_readers = 3"
    # Batches small enough that a batch ends between the sizes and the bytes of a row, which waits for the next.
    compare_with_cpu "SELECT max(s), min(t) FROM gpu_min_max_strings $SETTINGS, gpu_aggregation_batch_bytes = 65536"
    compare_with_cpu "SELECT k, max(s) FROM gpu_min_max_strings GROUP BY k ORDER BY k $SETTINGS, gpu_aggregation_batch_bytes = 65536"
done

# A read of strings with a `PREWHERE` is not aggregated from compressed blocks.
device_step "SELECT min(s) FROM gpu_min_max_strings WHERE k = 3" blocks

# Without keys and with `device`, the strings' bytes and sizes are both expanded on the device, and the host
# decompresses no block at all; with `host`, it decompresses them all.
for mode in device host; do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${mode}"
    $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query_id "$query_id" --query "
        SELECT min(s), max(t) FROM gpu_min_max_strings FORMAT Null SETTINGS gpu_aggregation_decompression = '$mode'"

    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "$mode ok"
        continue
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT '$mode', (ProfileEvents['CompressedReadBufferBlocks'] = 0) = ('$mode' = 'device') ? 'ok' : 'wrong side'
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'" | tr '\t' ' '
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_min_max_strings"
