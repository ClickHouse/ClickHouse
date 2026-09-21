#!/usr/bin/env bash

# Checks that `allow_experimental_gpu_aggregation` returns what the CPU returns.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially. That is what makes
# this test runnable everywhere: it becomes a real comparison exactly on the machines that can do
# one, and says nothing about the rest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_sum (u8 UInt8, u64 UInt64, i32 Int32, i64 Int64, f32 Float32, f64 Float64,
                          n Nullable(UInt64), d Decimal64(2), k UInt8)
    ENGINE = MergeTree ORDER BY tuple();

    -- The float columns hold multiples of an eighth, so that both sums are exact in a double
    -- whatever order the values are added in: the device reduces them as a tree and the CPU adds
    -- them in order, and only exactly representable sums let the two be compared for equality.
    INSERT INTO gpu_sum
    SELECT number % 251, number * 100000000, -number, number - 500000, number / 8, number / 8,
           if(number % 7 = 0, NULL, number), number / 100, number % 4
    FROM numbers(1000000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # The setting throws on a machine that has no usable device, which is the point of it - so ask
    # for one aggregation it would take over and see whether this machine is such a one.
    if $CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(u64) FROM gpu_sum" > /dev/null 2>&1; then
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
        echo "MISMATCH for '$query': $on_cpu on the CPU, $on_gpu on the GPU"
    fi
}

# Eligible: one `sum` of each supported argument type, and several of them at once.
compare_with_cpu "SELECT sum(u8) FROM gpu_sum"
compare_with_cpu "SELECT sum(u64) FROM gpu_sum"
compare_with_cpu "SELECT sum(i32) FROM gpu_sum"
compare_with_cpu "SELECT sum(i64) FROM gpu_sum"
compare_with_cpu "SELECT sum(f32) FROM gpu_sum"
compare_with_cpu "SELECT sum(f64) FROM gpu_sum"
compare_with_cpu "SELECT sum(u8), sum(i64), sum(f64) FROM gpu_sum"

# The sum of an expression, and of a constant, which arrive as a materialized and as a constant
# column respectively.
compare_with_cpu "SELECT sum(u8 + 1) FROM gpu_sum"
compare_with_cpu "SELECT sum(1) FROM gpu_sum"

# A `UInt64` sum that wraps around, which has to wrap the same way it does on the CPU.
compare_with_cpu "SELECT sum(u64) FROM gpu_sum WHERE u64 > 0"

# Eligible and reading nothing, either because the table is filtered away or because it is empty.
compare_with_cpu "SELECT sum(u64) FROM gpu_sum WHERE u8 > 255"
compare_with_cpu "SELECT sum(u64) FROM gpu_sum WHERE 0"
compare_with_cpu "SELECT sum(u64) FROM gpu_sum WHERE u8 > 255 SETTINGS empty_result_for_aggregation_by_empty_set = 1"

# Not eligible, and so computed on the CPU with the setting on all the same.
compare_with_cpu "SELECT sum(n) FROM gpu_sum"
compare_with_cpu "SELECT sum(d) FROM gpu_sum"
compare_with_cpu "SELECT sum(u64) FROM gpu_sum GROUP BY k ORDER BY k"
compare_with_cpu "SELECT sumIf(u64, k = 1) FROM gpu_sum"
compare_with_cpu "SELECT sum(DISTINCT u8) FROM gpu_sum"
compare_with_cpu "SELECT avg(u64), count(), min(u8), max(u8) FROM gpu_sum"
compare_with_cpu "SELECT sum(u64) FROM gpu_sum WITH TOTALS"
compare_with_cpu "SELECT sum(u64) FROM (SELECT * FROM gpu_sum UNION ALL SELECT * FROM gpu_sum)"
compare_with_cpu "SELECT sum(number) FROM numbers(1000)"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_sum"
