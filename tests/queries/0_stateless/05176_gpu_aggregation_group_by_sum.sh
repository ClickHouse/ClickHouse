#!/usr/bin/env bash

# Checks that `allow_experimental_gpu_aggregation` returns what the CPU returns for a `GROUP BY`.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially. That is what makes
# this test runnable everywhere: it becomes a real comparison exactly on the machines that can do
# one, and says nothing about the rest.
#
# The probe below is a keyless `sum` on purpose. It answers "can this process use a device at all",
# which is what decides whether the comparisons are real - and nothing more, so that a `GROUP BY`
# that fails on the device shows up as a mismatch here instead of quietly turning the whole test
# into a CPU-against-CPU tautology.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_group_by (k_u8 UInt8, k_u16 UInt16, k_u32 UInt32, k_u64 UInt64,
                               k_i8 Int8, k_i16 Int16, k_i32 Int32, k_i64 Int64,
                               k_f32 Float32, k_f64 Float64,
                               k_many UInt32, k_null Nullable(UInt8), k_lc LowCardinality(String),
                               k_str String,
                               v_u8 UInt8, v_u64 UInt64, v_i32 Int32, v_i64 Int64,
                               v_f32 Float32, v_f64 Float64,
                               v_null Nullable(UInt64), v_dec Decimal64(2))
    ENGINE = MergeTree ORDER BY tuple();

    -- The float columns hold multiples of an eighth, so that both sums are exact in a double
    -- whatever order the values are added in: the device reduces a group as a tree and the CPU
    -- adds it in order, and only exactly representable sums let the two be compared for equality.
    -- The float keys hold whole numbers, which are the same bits on both paths and so land in the
    -- same groups.
    INSERT INTO gpu_group_by
    SELECT number % 251, number % 1000, number % 7, number % 13,
           number % 100 - 50, number % 500 - 250, number % 11 - 5, number % 17 - 8,
           number % 5, number % 6,
           number % 50000, if(number % 7 = 0, NULL, number % 3), toString(number % 4),
           toString(number % 9),
           number % 251, number * 100000000, -number, number - 500000,
           number / 8, number / 8,
           if(number % 7 = 0, NULL, number), number / 100
    FROM numbers(1000000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # The setting throws on a machine that has no usable device, which is the point of it - so ask
    # for one aggregation it would take over and see whether this machine is such a one.
    if $CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_u64) FROM gpu_group_by" > /dev/null 2>&1; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1)
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

# Every comparison orders its groups. Neither path promises an order for `GROUP BY` - the device's
# is whatever the groupby produced, the CPU's whatever the hash table iterated - so without an
# `ORDER BY` a difference in output would say nothing about the sums.

# One key of each supported type.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u16, sum(v_u64) FROM gpu_group_by GROUP BY k_u16 ORDER BY k_u16"
compare_with_cpu "SELECT k_u32, sum(v_u64) FROM gpu_group_by GROUP BY k_u32 ORDER BY k_u32"
compare_with_cpu "SELECT k_u64, sum(v_u64) FROM gpu_group_by GROUP BY k_u64 ORDER BY k_u64"
compare_with_cpu "SELECT k_i8, sum(v_u64) FROM gpu_group_by GROUP BY k_i8 ORDER BY k_i8"
compare_with_cpu "SELECT k_i16, sum(v_u64) FROM gpu_group_by GROUP BY k_i16 ORDER BY k_i16"
compare_with_cpu "SELECT k_i32, sum(v_u64) FROM gpu_group_by GROUP BY k_i32 ORDER BY k_i32"
compare_with_cpu "SELECT k_i64, sum(v_u64) FROM gpu_group_by GROUP BY k_i64 ORDER BY k_i64"

# One `sum` of each supported argument type, grouped.
compare_with_cpu "SELECT k_u8, sum(v_u8) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_i32) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_i64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_f32) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_f64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"

# Several keys, and several `sum`s at once - including with the select list in an order other than
# the one the aggregation's own header uses, which puts the keys first in `GROUP BY` order.
compare_with_cpu "SELECT k_u8, k_i16, sum(v_u64) FROM gpu_group_by GROUP BY k_u8, k_i16 ORDER BY k_u8, k_i16"
compare_with_cpu "SELECT k_i16, k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8, k_i16 ORDER BY k_u8, k_i16"
compare_with_cpu "SELECT sum(v_u64), k_u8 FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u8), sum(v_i64), sum(v_f64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u32, k_i8, k_i32, sum(v_i64), sum(v_f32) FROM gpu_group_by GROUP BY k_u32, k_i8, k_i32 ORDER BY k_u32, k_i8, k_i32"

# Grouping by an expression, and by a column that takes one value, so the result is a single group.
compare_with_cpu "SELECT k_u8 % 3 AS m, sum(v_u64) FROM gpu_group_by GROUP BY m ORDER BY m"
compare_with_cpu "SELECT toUInt8(1) AS one, sum(v_u64) FROM gpu_group_by GROUP BY one ORDER BY one"

# A `UInt64` sum that wraps around inside every group - `k_u32` takes seven values, so a group is a
# seventh of the table and three times its sum passes 2^64. This is the case that pins down the
# wraparound: cuDF's groupby has no unsigned sum and accumulates the group in an `Int64`, and the
# eight bytes that come back have to be the ones the CPU's `UInt64` accumulator would have held.
compare_with_cpu "SELECT k_u32, sum(v_u64 * 3) FROM gpu_group_by GROUP BY k_u32 ORDER BY k_u32"

# Many distinct keys, with a batch small enough that the partial results of several batches have to
# be merged on the device - the interesting path, and the one a single-batch query never reaches.
compare_with_cpu "SELECT k_many, sum(v_i64) FROM gpu_group_by GROUP BY k_many ORDER BY k_many SETTINGS gpu_aggregation_batch_bytes = 65536"
compare_with_cpu "SELECT k_u8, sum(v_f64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8 SETTINGS gpu_aggregation_batch_bytes = 4096"

# Eligible and reading nothing. A keyed aggregation over no rows has no groups and so returns no
# rows, which `empty_result_for_aggregation_by_empty_set` does not change - that setting is about
# the one row a keyless aggregation returns for the empty set.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by WHERE k_u8 > 255 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by WHERE 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by WHERE 0 GROUP BY k_u8 ORDER BY k_u8 SETTINGS empty_result_for_aggregation_by_empty_set = 1"

# Not eligible, and so computed on the CPU with the setting on all the same.
#
# A float key is among these, and for a reason worth spelling out: ClickHouse groups a `Float64` by
# its eight bytes, so `0.0` and `-0.0` are two groups, while cuDF's hash groupby compares float
# keys with IEEE equality and makes them one. The last of these three pins that difference down, so
# that anyone who makes float keys eligible has to deal with it rather than discover it.
compare_with_cpu "SELECT k_f32, sum(v_u64) FROM gpu_group_by GROUP BY k_f32 ORDER BY k_f32"
compare_with_cpu "SELECT k_u32, k_f64, sum(v_u64) FROM gpu_group_by GROUP BY k_u32, k_f64 ORDER BY k_u32, k_f64"
compare_with_cpu "SELECT z, count(), sum(v_u64) FROM (SELECT if(k_i8 < 0, -0.0, 0.0) AS z, v_u64 FROM gpu_group_by) GROUP BY z ORDER BY reinterpretAsUInt64(z)"
compare_with_cpu "SELECT k_null, sum(v_u64) FROM gpu_group_by GROUP BY k_null ORDER BY k_null"
compare_with_cpu "SELECT k_lc, sum(v_u64) FROM gpu_group_by GROUP BY k_lc ORDER BY k_lc"
compare_with_cpu "SELECT k_str, sum(v_u64) FROM gpu_group_by GROUP BY k_str ORDER BY k_str"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8 SETTINGS group_by_use_nulls = 1"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8 WITH ROLLUP ORDER BY 1, 2"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8 WITH CUBE ORDER BY 1, 2"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY GROUPING SETS ((k_u8), ()) ORDER BY 1, 2"
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8 WITH TOTALS ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_null) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_dec) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sumIf(v_u64, k_i8 > 0) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, avg(v_u64), count(), min(v_u8) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8"
# A group-count limit that cannot be reached, so that the result is the same on both paths and the
# comparison is about eligibility rather than about which keys an overflow happened to keep.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_group_by GROUP BY k_u8 ORDER BY k_u8 SETTINGS max_rows_to_group_by = 1000000, group_by_overflow_mode = 'throw'"
compare_with_cpu "SELECT number % 7 AS k, sum(number) FROM numbers(1000) GROUP BY k ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_group_by"
