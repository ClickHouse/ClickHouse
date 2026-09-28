#!/usr/bin/env bash

# Checks that `join_algorithm = 'gpu_hash'` returns what the CPU returns.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially. That is what makes
# this test runnable everywhere: it becomes a real comparison exactly on the machines that can do
# one, and says nothing about the rest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_join_left (k UInt32, k8 UInt8, k16 Int16, k64 Int64, kf Float64, kn Nullable(UInt32), ks String,
                                v_i32 Int32, v_f64 Float64, v_u8 UInt8)
    ENGINE = MergeTree ORDER BY tuple();

    CREATE TABLE gpu_join_right (k UInt32, k8 UInt8, k16 Int16, k64 Int64, kf Float64, kn Nullable(UInt32), ks String,
                                 w_u64 UInt64, w_f32 Float32, w_i16 Int16, w_str String)
    ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO gpu_join_left
    SELECT number % 10000, number % 256, number % 1000 - 500, number - 50000, number % 7,
           if(number % 5 = 0, NULL, number % 100), toString(number % 10),
           -number, number / 8, number % 3
    FROM numbers(100000);

    INSERT INTO gpu_join_right
    SELECT number % 2500, number % 256, number % 1000 - 500, number * 10 - 50000, number % 7,
           if(number % 3 = 0, NULL, number % 100), toString(number % 10),
           number * 3, number / 4, number % 300 - 150, toString(number)
    FROM numbers(5000);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # A device that fails for any other reason than being absent is a bug, and is reported as one
    # rather than being compared with itself on the CPU.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --join_algorithm gpu_hash \
        --query "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        GPU_SETTINGS=(--join_algorithm "gpu_hash,hash" --enable_join_runtime_filters 0)
    elif [[ "$PROBE_ERROR" != *"Cannot join on a GPU"* ]]; then
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

function check_on_gpu()
{
    local expected="$1"
    local query="$2"

    if [ ${#GPU_SETTINGS[@]} -eq 0 ]; then
        echo "ok"
        return
    fi

    local actual=no
    if $CLICKHOUSE_CLIENT "${GPU_SETTINGS[@]}" --query "EXPLAIN actions = 1 $query" | grep -q "Algorithm: GPUHashJoin"; then
        actual=yes
    fi

    if [ "$actual" == "$expected" ]; then
        echo "ok"
    else
        echo "'$query' is planned for the GPU: $actual, expected $expected"
    fi
}

check_on_gpu yes "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT count(), sum(v_i32), sum(w_u64), sum(w_f32), min(w_i16), max(w_i16) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT l.k, v_i32, w_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k ORDER BY l.k, v_i32, w_u64 LIMIT 20"
compare_with_cpu "SELECT l.k, v_i32, w_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k ORDER BY l.k DESC, v_i32, w_u64 LIMIT 20"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r USING (k)"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l JOIN gpu_join_right AS r ON l.k = r.k"

compare_with_cpu "SELECT count(), sum(w_u64), sum(v_i32) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k8 = r.k8 WHERE l.k < 100"
compare_with_cpu "SELECT count(), sum(w_u64), sum(v_i32) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k16 = r.k16 WHERE l.k < 100"
compare_with_cpu "SELECT count(), sum(w_u64), sum(v_i32) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k64 = r.k64"
compare_with_cpu "SELECT l.k64, w_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k64 = r.k64 ORDER BY l.k64, w_u64 LIMIT 20"

compare_with_cpu "SELECT sum(w_u64), sum(w_f32), sum(w_i16), sum(v_u8), sum(v_f64), sum(v_i32) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT count(), sum(v_f64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT r.k, count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k GROUP BY r.k ORDER BY r.k LIMIT 10"
compare_with_cpu "SELECT l.k, r.k, count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k GROUP BY l.k, r.k ORDER BY l.k LIMIT 10"

compare_with_cpu "SELECT count(), sum(v_i32), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k SETTINGS max_block_size = 1000"
compare_with_cpu "SELECT l.k, v_i32, w_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k ORDER BY l.k, v_i32, w_u64 LIMIT 20 SETTINGS max_block_size = 777"

compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k WHERE l.k >= 5000"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN (SELECT * FROM gpu_join_right WHERE 0) AS r ON l.k = r.k"
compare_with_cpu "SELECT count(), sum(w_u64) FROM (SELECT * FROM gpu_join_left WHERE 0) AS l INNER JOIN gpu_join_right AS r ON l.k = r.k"

check_on_gpu no "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.kn = r.kn"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.kn = r.kn"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.ks = r.ks WHERE l.k < 100"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.kf = r.kf WHERE l.k < 20"
compare_with_cpu "SELECT count(), max(w_str) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k AND l.k8 = r.k8"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l LEFT JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l ANY INNER JOIN gpu_join_right AS r ON l.k = r.k"
compare_with_cpu "SELECT count(), sum(w_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k = r.k AND l.v_u8 > r.w_i16"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_join_left; DROP TABLE gpu_join_right"
