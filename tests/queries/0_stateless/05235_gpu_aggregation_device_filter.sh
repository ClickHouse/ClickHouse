#!/usr/bin/env bash

# Checks that a `GROUP BY` with a `WHERE` the device evaluates returns what the CPU returns.
#
# A `WHERE` over a `MergeTree` table becomes a `PREWHERE` in the read, and a keyed aggregation over
# compressed blocks compiles it for the device where it is made of comparisons, `and`, `or` and
# `not` over integer and float columns and constants. Anything else keeps the query on the CPU,
# and so a comparison here holds for both kinds of predicate; which kind a predicate is shows in
# the plan, which `check_plan` looks at.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU. The probe is a keyless `sum`: it answers
# whether this process can use a device at all, so that a `WHERE` that fails on the device shows
# up as a mismatch instead of a tautology.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_device_filter (k_u8 UInt8, k_i8 Int8, k_many UInt32,
                                    v_u8 UInt8, v_u16 UInt16, v_i32 Int32, v_i64 Int64, v_u64 UInt64,
                                    v_f32 Float32, v_f64 Float64, v_nan Float64, v_null Nullable(UInt8))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;

    -- Wide parts with every column stored plainly, since only those are read as compressed blocks,
    -- and the device evaluates a predicate only on that path; the test runner would otherwise
    -- draw these settings at random. The float columns hold multiples of an eighth, so that the sums
    -- are exact in a double in any order of addition. v_nan is NaN on every thousandth row: a
    -- comparison with NaN is false on the CPU and must be so on the device. Three inserts make
    -- three parts.
    INSERT INTO gpu_device_filter
    SELECT number % 251, number % 100 - 50, number % 50000,
           number % 251, number % 60000, -number, number - 500000, number * 3,
           number / 8, number / 8, if(number % 1000 = 0, nan, number / 8), if(number % 7 = 0, NULL, number % 3)
    FROM numbers(300000);
    INSERT INTO gpu_device_filter
    SELECT number % 251, number % 100 - 50, number % 50000,
           number % 251, number % 60000, -number, number - 500000, number * 3,
           number / 8, number / 8, if(number % 1000 = 0, nan, number / 8), if(number % 7 = 0, NULL, number % 3)
    FROM numbers(300000, 300000);
    INSERT INTO gpu_device_filter
    SELECT number % 251, number % 100 - 50, number % 50000,
           number % 251, number % 60000, -number, number - 500000, number * 3,
           number / 8, number / 8, if(number % 1000 = 0, nan, number / 8), if(number % 7 = 0, NULL, number % 3)
    FROM numbers(600000, 400000);
"

GPU_SETTINGS=()
HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_u64) FROM gpu_device_filter" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        # The device is offered a `PREWHERE` only, and the test runner may have turned off the
        # setting that makes one of a `WHERE`.
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1 --optimize_move_to_prewhere 1)
        HAS_GPU=1
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

# Whether the plan evaluates the predicate on the device. On a machine without a device the plan
# is the CPU's, and the expected line is printed as it is.
function check_plan()
{
    local expected="$1"
    local query="$2"

    if [ "$HAS_GPU" == "1" ]; then
        if $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN actions = 1 $query" | grep -q "Filter on the device"; then
            echo "filter on the device"
        else
            echo "filter on the CPU"
        fi
    else
        echo "$expected"
    fi
}

# Comparisons of a column with a constant, of each kind.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 200 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u16 <= 30000 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_i32 > -500000 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_i64 >= 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE k_i8 = -1 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE k_i8 != 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE 100 > v_u8 GROUP BY k_u8 ORDER BY k_u8"

# Floats: against a float constant, against an integer constant a double holds exactly, and a
# `Float32` column, which is widened to a double before it compares.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 > 50000.5 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 = 0.125 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 < 100 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f32 < 0.1 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f32 >= 12345.125 GROUP BY k_u8 ORDER BY k_u8"

# NaN compares as false with everything but `!=`.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_nan < 100 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_nan >= 100 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_nan != 100 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_nan = v_nan GROUP BY k_u8 ORDER BY k_u8"

# Two columns, with a sign on one side: a negative `Int8` is less than any `UInt8`.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE k_i8 < v_u8 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE k_i8 = v_u8 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_i64 < v_u64 GROUP BY k_u8 ORDER BY k_u8"

# `and`, `or` and `not`, and a column standing for itself.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 100 AND k_i8 > 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 100 OR k_i8 > 40 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE NOT (v_u8 < 100) GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 100 AND k_i8 > -20 AND v_f64 < 90000 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE (v_u8 < 100 OR k_i8 > 40) AND NOT (v_u16 = 7) GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 AND k_i8 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE NOT v_u8 GROUP BY k_u8 ORDER BY k_u8"

# A predicate over a key, over an aggregated column, and over a column read for nothing else.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE k_u8 < 100 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u64 < 1000000 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64), min(v_i32), max(v_f64) FROM gpu_device_filter WHERE v_u16 < 40000 GROUP BY k_u8 ORDER BY k_u8"

# Many groups, with a batch small enough that several are merged on the device.
compare_with_cpu "SELECT k_many, sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 128 GROUP BY k_many ORDER BY k_many SETTINGS gpu_aggregation_batch_bytes = 65536"

# Nothing passes, and everything passes.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 > 255 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 >= 0 GROUP BY k_u8 ORDER BY k_u8"

# Predicates the device does not take, and so computed on the CPU with the setting on all the same:
# a function other than a comparison, an integer column against a float, an integer constant a
# double does not hold exactly, a nullable column, and a keyless aggregation.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 % 2 = 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 < v_u8 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_i64 < 0.5 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 < 9007199254740993 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_null > 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 200"

check_plan "filter on the device" "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 < 200 AND k_i8 > 0 GROUP BY k_u8"
check_plan "filter on the device" "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 < 100 GROUP BY k_u8"
check_plan "filter on the CPU" "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_u8 % 2 = 0 GROUP BY k_u8"
check_plan "filter on the CPU" "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_f64 < v_u8 GROUP BY k_u8"
check_plan "filter on the CPU" "SELECT k_u8, sum(v_u64) FROM gpu_device_filter WHERE v_null > 0 GROUP BY k_u8"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_device_filter"
