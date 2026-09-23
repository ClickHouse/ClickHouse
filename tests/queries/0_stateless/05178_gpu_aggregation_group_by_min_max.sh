#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_group_by_min_max (k_u8 UInt8, k_u32 UInt32, k_i16 Int16, k_i64 Int64,
                                       k_many UInt32, k_f64 Float64, k_null Nullable(UInt8),
                                       k_str String,
                                       v_u8 UInt8, v_u16 UInt16, v_u32 UInt32, v_u64 UInt64,
                                       v_i8 Int8, v_i16 Int16, v_i32 Int32, v_i64 Int64,
                                       v_f32 Float32, v_f64 Float64,
                                       v_null Nullable(UInt64), v_dec Decimal64(2), v_str String)
    ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO gpu_group_by_min_max
    SELECT number % 251, number % 7, number % 11 - 5, number % 17 - 8,
           number % 50000, number % 6, if(number % 7 = 0, NULL, number % 3),
           toString(number % 9),
           number % 251, number % 60000, number, number * 100000000,
           number % 100 - 50, number % 500 - 250, -number, number - 500000,
           number / 8 - 60000, number / 8 - 60000,
           if(number % 7 = 0, NULL, number), number / 100, toString(number)
    FROM numbers(1000000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_u64) FROM gpu_group_by_min_max" 2>&1 > /dev/null)
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

# Every comparison orders its groups, because neither path promises an order for `GROUP BY`.

# A grouped `min` and `max` of each supported argument type. These keep the argument's type, where
# a grouped `sum` widens to 64 bits, so the device has to copy back a narrower value per group.
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_u16), max(v_u16) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_u32), max(v_u32) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_u64), max(v_u64) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_i8), max(v_i8) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_i16), max(v_i16) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_i32), max(v_i32) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_i64), max(v_i64) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_f32), max(v_f32) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_f64), max(v_f64) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"

# A key of each supported type.
compare_with_cpu "SELECT k_u32, min(v_i32), max(v_i32) FROM gpu_group_by_min_max GROUP BY k_u32 ORDER BY k_u32"
compare_with_cpu "SELECT k_i16, min(v_i32), max(v_i32) FROM gpu_group_by_min_max GROUP BY k_i16 ORDER BY k_i16"
compare_with_cpu "SELECT k_i64, min(v_i32), max(v_i32) FROM gpu_group_by_min_max GROUP BY k_i64 ORDER BY k_i64"

# Several keys, and a `min`, a `max` and a `sum` of different widths side by side - which is where
# each value column comes back in a type of its own.
compare_with_cpu "SELECT k_u8, k_i16, min(v_u8), max(v_i64) FROM gpu_group_by_min_max GROUP BY k_u8, k_i16 ORDER BY k_u8, k_i16"
compare_with_cpu "SELECT k_u8, min(v_u8), sum(v_u8), max(v_f32) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64), min(v_i8), max(v_u16), sum(v_f64) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT min(v_u8), k_u8, max(v_u8) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"

# Grouping by an expression, and by a column that takes one value.
compare_with_cpu "SELECT k_u8 % 3 AS m, min(v_i32), max(v_i32) FROM gpu_group_by_min_max GROUP BY m ORDER BY m"
compare_with_cpu "SELECT toUInt8(1) AS one, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY one ORDER BY one"

# Many distinct keys, with a batch small enough that the partial results of several batches have to
# be merged on the device - a minimum of minima and a maximum of maxima.
compare_with_cpu "SELECT k_many, min(v_i64), max(v_i64) FROM gpu_group_by_min_max GROUP BY k_many ORDER BY k_many SETTINGS gpu_aggregation_batch_bytes = 65536"
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8 SETTINGS gpu_aggregation_batch_bytes = 4096"
compare_with_cpu "SELECT k_many, min(v_f32), max(v_u16), sum(v_u8) FROM gpu_group_by_min_max GROUP BY k_many ORDER BY k_many SETTINGS gpu_aggregation_batch_bytes = 4096"

# Eligible and reading nothing.
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max WHERE k_u8 > 255 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max WHERE 0 GROUP BY k_u8 ORDER BY k_u8"

# Not eligible, and so computed on the CPU with the setting on all the same.
compare_with_cpu "SELECT k_f64, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_f64 ORDER BY k_f64"
compare_with_cpu "SELECT k_null, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_null ORDER BY k_null"
compare_with_cpu "SELECT k_str, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_str ORDER BY k_str"
compare_with_cpu "SELECT k_u8, min(v_null), max(v_null) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_dec), max(v_dec) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_str), max(v_str) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, minIf(v_u8, k_i16 > 0), maxIf(v_u8, k_i16 > 0) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, argMin(v_u8, v_i32), argMax(v_u8, v_i32) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8), count() FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_u8 ORDER BY k_u8 SETTINGS group_by_use_nulls = 1"
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_u8 WITH ROLLUP ORDER BY 1, 2, 3"
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_group_by_min_max GROUP BY k_u8 WITH TOTALS ORDER BY k_u8"
compare_with_cpu "SELECT number % 7 AS k, min(number), max(number) FROM numbers(1000) GROUP BY k ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_group_by_min_max"
