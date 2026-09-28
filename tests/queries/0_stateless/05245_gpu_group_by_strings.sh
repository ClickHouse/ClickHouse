#!/usr/bin/env bash

# Checks a `GROUP BY` by `String` keys on the device against the CPU: over the blocks the pipeline
# reads, and over compressed blocks, with the strings' bytes expanded on the device and on the host.
# The strings are empty, short and long, so that their bytes do not end where a compressed block
# does, and there are several parts, so that the groups of the parts merge.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_group_by_strings (s String, t String, k UInt16, v UInt64, i Int32, b UInt8)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
        serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

    SYSTEM STOP MERGES gpu_group_by_strings;
"

for part in 0 1 2 3; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO gpu_group_by_strings
        SELECT
            multiIf(number % 7 = 0, '', number % 7 = 1, repeat('long string ', 30 + number % 5), 'key_' || toString(number % 997)),
            toString(cityHash64(number % 50)),
            number % 13,
            cityHash64(number),
            toInt32(number % 100000) - 50000,
            number % 256
        FROM numbers($part * 400000, 400000 + $part)"
done

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v) FROM gpu_group_by_strings" 2>&1 > /dev/null)
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
device_step "SELECT s, sum(v) FROM gpu_group_by_strings $WHERE GROUP BY s $SETTINGS" blocks
compare_with_cpu "SELECT s, sum(v), min(i), max(b) FROM gpu_group_by_strings $WHERE GROUP BY s ORDER BY s $SETTINGS"
compare_with_cpu "SELECT s, k, sum(v), max(i) FROM gpu_group_by_strings $WHERE GROUP BY s, k ORDER BY s, k $SETTINGS"
compare_with_cpu "SELECT t, s, min(v) FROM gpu_group_by_strings $WHERE GROUP BY t, s ORDER BY t, s $SETTINGS"
compare_with_cpu "SELECT count(), sum(length(s)), sum(m) FROM (SELECT s, sum(i) AS m FROM gpu_group_by_strings $WHERE GROUP BY s) $SETTINGS"
# Batches small enough that the groups of many of them merge.
compare_with_cpu "SELECT s, sum(v) FROM gpu_group_by_strings $WHERE GROUP BY s ORDER BY s $SETTINGS, gpu_aggregation_batch_bytes = 65536"

echo "- compressed"

device_step "SELECT s, sum(v) FROM gpu_group_by_strings GROUP BY s" compressed
for mode in device host; do
    SETTINGS="SETTINGS gpu_aggregation_decompression = '$mode'"
    compare_with_cpu "SELECT s, sum(v), min(i), max(b) FROM gpu_group_by_strings GROUP BY s ORDER BY s $SETTINGS"
    compare_with_cpu "SELECT s, k, sum(v), max(i) FROM gpu_group_by_strings GROUP BY s, k ORDER BY s, k $SETTINGS"
    compare_with_cpu "SELECT t, s, min(v) FROM gpu_group_by_strings GROUP BY t, s ORDER BY t, s $SETTINGS"
    compare_with_cpu "SELECT k, s, sum(i) FROM gpu_group_by_strings GROUP BY k, s ORDER BY k, s $SETTINGS, gpu_aggregation_readers = 3"
    # Pieces of a part grouped as they arrive, and the bytes of the grouped rows dropped from the
    # device before the part ends.
    compare_with_cpu "SELECT s, t, sum(v), min(b) FROM gpu_group_by_strings GROUP BY s, t ORDER BY s, t $SETTINGS, gpu_aggregation_batch_bytes = 65536"
done

# A float is not reduced by a `GROUP BY` by strings on the device, and neither is a read with a
# `PREWHERE` for it: both are aggregated on the CPU.
device_step "SELECT s, sum(toFloat64(v)) FROM gpu_group_by_strings GROUP BY s" cpu
device_step "SELECT s, sum(v) FROM gpu_group_by_strings WHERE k = 3 GROUP BY s" blocks

# `device` expands the strings' bytes on the device and `host` does not, as `GPUDecompressionBytes`
# counts what the device expanded.
for mode in device host; do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${mode}"
    $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query_id "$query_id" --query "
        SELECT s, sum(v) FROM gpu_group_by_strings GROUP BY s FORMAT Null SETTINGS gpu_aggregation_decompression = '$mode'"

    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "$mode ok"
        continue
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT '$mode', if(ProfileEvents['GPUDecompressionBytes'] > 0, 'device', 'host') = if('$mode' = 'device', 'device', 'host') ? 'ok' : 'wrong side'
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'" | tr '\t' ' '
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_group_by_strings"
