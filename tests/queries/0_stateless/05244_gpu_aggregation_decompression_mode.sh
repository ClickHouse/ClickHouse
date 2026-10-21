#!/usr/bin/env bash

# Checks the aggregations over compressed blocks against the CPU with every value of
# `gpu_aggregation_decompression`, and that `device` and `host` expand the columns where they say:
# the device counts what it expanded in `GPUDecompressionBytes`.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Several parts, so that `auto` has parts to move its threshold between. `v_random` is hashes,
# which LZ4 leaves as long as they are; `v_dense` and the key shrink well.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_decompression_mode (k UInt32, v_random UInt64, v_dense UInt64, v_i32 Int32)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

    SYSTEM STOP MERGES gpu_decompression_mode;
"

for part in 0 1 2 3 4 5 6 7; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO gpu_decompression_mode
        SELECT number % 1000, cityHash64(number), number, toInt32(number % 100000 - 50000)
        FROM numbers($part * 300000, 300000 + $part)"
done

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_dense) FROM gpu_decompression_mode" 2>&1 > /dev/null)
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

for mode in ratio auto device host; do
    # Reduced a part at a time.
    compare_with_cpu "SELECT sum(v_random), sum(v_dense), min(v_i32), max(v_random) FROM gpu_decompression_mode SETTINGS gpu_aggregation_decompression = '$mode'"

    # Grouped from every column at once, over several readers.
    compare_with_cpu "SELECT k, sum(v_random), sum(v_dense), min(v_i32) FROM gpu_decompression_mode GROUP BY k ORDER BY k SETTINGS gpu_aggregation_decompression = '$mode'"
    compare_with_cpu "SELECT k, max(v_random), min(v_dense) FROM gpu_decompression_mode GROUP BY k ORDER BY k SETTINGS gpu_aggregation_decompression = '$mode', gpu_aggregation_readers = 4"
done

# `auto` starts from the threshold and moves from wherever it starts.
for ratio in 0 1; do
    compare_with_cpu "SELECT k, sum(v_random), max(v_dense) FROM gpu_decompression_mode GROUP BY k ORDER BY k SETTINGS gpu_aggregation_decompression = 'auto', gpu_aggregation_device_decompression_max_ratio = $ratio"
done

# `device` expands every column on the device and `host` none, whatever the threshold says.
for mode in device host; do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${mode}"
    $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query_id "$query_id" --query "
        SELECT k, sum(v_random), sum(v_dense) FROM gpu_decompression_mode GROUP BY k FORMAT Null
        SETTINGS gpu_aggregation_decompression = '$mode', gpu_aggregation_device_decompression_max_ratio = 0.5"

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

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_decompression_mode"
