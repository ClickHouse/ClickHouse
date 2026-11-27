#!/usr/bin/env bash

# Checks the `GROUP BY` that reads a table's compressed blocks and has the device expand and group
# them - the path `ReadFromGPUCompressedColumns` takes when the aggregation has keys - against the
# CPU.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially. That is what makes
# this test runnable everywhere: it becomes a real comparison exactly on the machines that can do
# one, and says nothing about the rest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Several parts, so that the groups of one part have to meet the groups of the others on the
# device, and a block of a `UInt8` column covers eight times the rows a block of a `UInt64` does,
# so that the columns of a part arrive on the device unevenly.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_compressed_group_by (k_u8 UInt8, k_i16 Int16, k_u32 UInt32, k_i64 Int64, k_str String,
                                          v_u64 UInt64, v_i64 Int64, v_f64 Float64,
                                          v_u8 UInt8, v_i32 Int32, v_f32 Float32)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

    SYSTEM STOP MERGES gpu_compressed_group_by;

    -- The float column holds multiples of an eighth, so that its sums are exact in a double
    -- whatever order the values are added in.
    INSERT INTO gpu_compressed_group_by
    SELECT number % 251, number % 1000 - 500, number % 50000, number - 300000, toString(number % 4),
           number * 100000000, number - 150000, number / 8,
           number % 256, toInt32(number % 2000000 - 1000000), number / 4
    FROM numbers(0, 300000);

    INSERT INTO gpu_compressed_group_by
    SELECT number % 251, number % 1000 - 500, number % 50000, number - 300000, toString(number % 4),
           number * 100000000, number - 150000, number / 8,
           number % 256, toInt32(number % 2000000 - 1000000), number / 4
    FROM numbers(300000, 300000);

    INSERT INTO gpu_compressed_group_by
    SELECT number % 251, number % 1000 - 500, number % 50000, number - 300000, toString(number % 4),
           number * 100000000, number - 150000, number / 8,
           number % 256, toInt32(number % 2000000 - 1000000), number / 4
    FROM numbers(600000, 100000);
"

GPU_SETTINGS=()
HAS_GPU=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --allow_experimental_gpu_aggregation 1 \
        --query "SELECT sum(v_u64) FROM gpu_compressed_group_by" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--allow_experimental_gpu_aggregation 1)
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

# Whether the plan reads compressed blocks for a keyed aggregation, and leaves one alone whose
# filter the device does not evaluate - a `WHERE` of comparisons and logical functions it takes,
# which `05235_gpu_aggregation_device_filter` is about. On a machine without a device the plan is
# the CPU's, and the two lines are printed as they are.
function check_plan()
{
    if [ "$HAS_GPU" == "1" ]; then
        if $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN SELECT k_u8, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_u8" | grep -q "ReadFromGPUCompressedColumns"; then
            echo "keyed aggregation reads compressed blocks"
        else
            echo "keyed aggregation does NOT read compressed blocks"
        fi
        if $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN SELECT k_u8, sum(v_u64) FROM gpu_compressed_group_by WHERE k_u8 % 2 = 0 GROUP BY k_u8" | grep -q "ReadFromGPUCompressedColumns"; then
            echo "filtered aggregation reads compressed blocks"
        else
            echo "filtered aggregation reads rows"
        fi
    else
        echo "keyed aggregation reads compressed blocks"
        echo "filtered aggregation reads rows"
    fi
}

check_plan

# One key of each width.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_i16, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_i16 ORDER BY k_i16"
compare_with_cpu "SELECT k_u32, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_u32 ORDER BY k_u32"
compare_with_cpu "SELECT k_i64, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_i64 ORDER BY k_i64"

# Sums that keep their argument's type, which is what lets the step emit them in the read's header.
compare_with_cpu "SELECT k_u8, sum(v_i64) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_f64) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"

# Minimums and maximums of narrow types.
compare_with_cpu "SELECT k_u8, min(v_u8), max(v_u8) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_i16, min(v_i32), max(v_f32) FROM gpu_compressed_group_by GROUP BY k_i16 ORDER BY k_i16"

# Several keys and several aggregates, in an order other than the aggregation's own.
compare_with_cpu "SELECT k_u8, k_i16, sum(v_u64), min(v_i32), max(v_f64) FROM gpu_compressed_group_by GROUP BY k_u8, k_i16 ORDER BY k_u8, k_i16"
compare_with_cpu "SELECT sum(v_f64), k_i16, max(v_u8), k_u8 FROM gpu_compressed_group_by GROUP BY k_u8, k_i16 ORDER BY k_u8, k_i16"
compare_with_cpu "SELECT k_u32, k_i64, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_u32, k_i64 ORDER BY k_u32, k_i64"

# A batch small enough that a part is grouped in several pieces and the rows already grouped are
# dropped from the device between them.
compare_with_cpu "SELECT k_u32, sum(v_u64), sum(v_f64) FROM gpu_compressed_group_by GROUP BY k_u32 ORDER BY k_u32 SETTINGS gpu_aggregation_batch_bytes = 65536"
compare_with_cpu "SELECT k_u8, min(v_i32) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8 SETTINGS gpu_aggregation_batch_bytes = 4096"

# Not eligible for the compressed read, and so grouped from rows on whichever path takes them.
compare_with_cpu "SELECT k_u8, sum(v_u64) FROM gpu_compressed_group_by WHERE k_i16 > 0 GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u8) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_str, sum(v_u64) FROM gpu_compressed_group_by GROUP BY k_str ORDER BY k_str"
compare_with_cpu "SELECT k_u8, sum(k_u8) FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"
compare_with_cpu "SELECT k_u8, sum(v_u64), count() FROM gpu_compressed_group_by GROUP BY k_u8 ORDER BY k_u8"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_compressed_group_by"
