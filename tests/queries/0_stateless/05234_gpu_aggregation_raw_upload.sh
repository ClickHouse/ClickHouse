#!/usr/bin/env bash

# Checks the aggregations over compressed blocks against the CPU with every setting of
# `gpu_aggregation_device_decompression_max_ratio`: a column that compression barely shrinks is
# expanded on the CPU and sent whole, one that compresses well is expanded on the device, and the
# threshold decides which is which.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `v_random` is hashes, which LZ4 leaves as long as they are; `v_dense` and the key shrink well.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_raw_upload (k UInt32, v_random UInt64, v_dense UInt64, v_i32 Int32)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

    SYSTEM STOP MERGES gpu_raw_upload;

    INSERT INTO gpu_raw_upload
    SELECT number % 1000, cityHash64(number), number, toInt32(number % 100000 - 50000)
    FROM numbers(0, 1000000);

    INSERT INTO gpu_raw_upload
    SELECT number % 1000, cityHash64(number), number, toInt32(number % 100000 - 50000)
    FROM numbers(1000000, 700007);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_dense) FROM gpu_raw_upload" 2>&1 > /dev/null)
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

for ratio in 0 0.5 1; do
    # Reduced a part at a time.
    compare_with_cpu "SELECT sum(v_random), sum(v_dense), min(v_i32), max(v_random) FROM gpu_raw_upload SETTINGS gpu_aggregation_device_decompression_max_ratio = $ratio"
    compare_with_cpu "SELECT max(v_dense), min(v_random) FROM gpu_raw_upload SETTINGS gpu_aggregation_device_decompression_max_ratio = $ratio, gpu_aggregation_batch_bytes = 65536"

    # Grouped from every column at once, the columns arriving by different routes.
    compare_with_cpu "SELECT k, sum(v_random), sum(v_dense), min(v_i32) FROM gpu_raw_upload GROUP BY k ORDER BY k SETTINGS gpu_aggregation_device_decompression_max_ratio = $ratio"
    compare_with_cpu "SELECT k, max(v_random), min(v_dense) FROM gpu_raw_upload GROUP BY k ORDER BY k SETTINGS gpu_aggregation_device_decompression_max_ratio = $ratio, gpu_aggregation_batch_bytes = 65536"
done

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_raw_upload"
