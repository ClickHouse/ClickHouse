#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_min_max (u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
                              i8 Int8, i16 Int16, i32 Int32, i64 Int64, f32 Float32, f64 Float64,
                              n Nullable(UInt64), d Decimal64(2), s String, k UInt8)
    ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO gpu_min_max
    SELECT number % 251, number % 60000, number, number * 100000000,
           number % 100 - 50, number % 500 - 250, -number, number - 500000,
           number / 8 - 60000, number / 8 - 60000,
           if(number % 7 = 0, NULL, number), number / 100, toString(number), number % 4
    FROM numbers(1000000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(u64) FROM gpu_min_max" 2>&1 > /dev/null)
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
        echo "MISMATCH for '$query': $on_cpu on the CPU, $on_gpu on the GPU"
    fi
}

# Eligible: a `min` and a `max` of each supported argument type.
compare_with_cpu "SELECT min(u8), max(u8) FROM gpu_min_max"
compare_with_cpu "SELECT min(u16), max(u16) FROM gpu_min_max"
compare_with_cpu "SELECT min(u32), max(u32) FROM gpu_min_max"
compare_with_cpu "SELECT min(u64), max(u64) FROM gpu_min_max"
compare_with_cpu "SELECT min(i8), max(i8) FROM gpu_min_max"
compare_with_cpu "SELECT min(i16), max(i16) FROM gpu_min_max"
compare_with_cpu "SELECT min(i32), max(i32) FROM gpu_min_max"
compare_with_cpu "SELECT min(i64), max(i64) FROM gpu_min_max"
compare_with_cpu "SELECT min(f32), max(f32) FROM gpu_min_max"
compare_with_cpu "SELECT min(f64), max(f64) FROM gpu_min_max"

# Several aggregates at once, mixed with a `sum`.
compare_with_cpu "SELECT min(u8), max(i64), min(f64) FROM gpu_min_max"
compare_with_cpu "SELECT min(i32), max(i32), sum(i32) FROM gpu_min_max"
compare_with_cpu "SELECT max(u8), min(u8), max(u8), min(u8) FROM gpu_min_max"
compare_with_cpu "SELECT sum(u8), min(u16), max(f32) FROM gpu_min_max"

# Of an expression and of a constant, which arrive as a materialized and as a constant column.
compare_with_cpu "SELECT min(u8 + 1), max(-i32) FROM gpu_min_max"
compare_with_cpu "SELECT min(1), max(1) FROM gpu_min_max"

# Batches small enough that several of them are reduced and their results combined on the host.
compare_with_cpu "SELECT min(i32), max(i32) FROM gpu_min_max SETTINGS gpu_aggregation_batch_bytes = 4096"
compare_with_cpu "SELECT min(u64), max(u64) FROM gpu_min_max SETTINGS gpu_aggregation_batch_bytes = 65536"
compare_with_cpu "SELECT min(f64), max(f64) FROM gpu_min_max SETTINGS gpu_aggregation_batch_bytes = 4096"
compare_with_cpu "SELECT min(u8), max(u8) FROM gpu_min_max SETTINGS gpu_aggregation_batch_bytes = 1024"

# Eligible and reading nothing, so that the result is the default value of the result type.
compare_with_cpu "SELECT min(u8), max(u8) FROM gpu_min_max WHERE u8 > 255"
compare_with_cpu "SELECT min(i64), max(f64) FROM gpu_min_max WHERE 0"
compare_with_cpu "SELECT min(u64), max(u64) FROM gpu_min_max WHERE u8 > 255 SETTINGS empty_result_for_aggregation_by_empty_set = 1"

# Not eligible, and so computed on the CPU with the setting on all the same.
compare_with_cpu "SELECT min(n), max(n) FROM gpu_min_max"
compare_with_cpu "SELECT min(d), max(d) FROM gpu_min_max"
compare_with_cpu "SELECT min(s), max(s) FROM gpu_min_max"
compare_with_cpu "SELECT minIf(u64, k = 1), maxIf(u64, k = 1) FROM gpu_min_max"
compare_with_cpu "SELECT argMin(u8, i32), argMax(u8, i32) FROM gpu_min_max"
compare_with_cpu "SELECT minOrNull(u8), maxOrNull(u8) FROM gpu_min_max"
compare_with_cpu "SELECT min(u8), max(u8), count() FROM gpu_min_max"
compare_with_cpu "SELECT min(u8), max(u8) FROM gpu_min_max WITH TOTALS"
compare_with_cpu "SELECT min(u8), max(u8) FROM (SELECT * FROM gpu_min_max UNION ALL SELECT * FROM gpu_min_max)"
compare_with_cpu "SELECT min(number), max(number) FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_min_max"
