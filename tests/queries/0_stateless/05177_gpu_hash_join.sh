#!/usr/bin/env bash

# Checks that `join_algorithm = 'gpu_hash'` returns what the CPU's hash join returns, and that the
# join shapes it cannot do raise an error instead of quietly using another algorithm.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied to
# the comparisons and both sides of every one of them run on the CPU - the checks hold trivially.
# That is what makes this test runnable everywhere: it becomes a real comparison exactly on the
# machines that can do one, and says nothing about the rest.
#
# The error checks need no such treatment and run everywhere as they are. `gpu_hash` is a value of
# `join_algorithm` in every build, and it is the only algorithm asked for in them - so a build
# without GPU support, a machine without a device, and a machine with one looking at a join it
# cannot do all reach the same "none of the algorithms enabled" from `chooseJoinAlgorithm`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE gpu_join_left (k_u8 UInt8, k_u16 UInt16, k_u32 UInt32, k_u64 UInt64,
                                k_i8 Int8, k_i16 Int16, k_i32 Int32, k_i64 Int64,
                                k_dup UInt32, k_none UInt32, k_second UInt32,
                                k_f64 Float64, k_str String, k_null Nullable(UInt32),
                                k_lc LowCardinality(String),
                                l_u64 UInt64, l_i32 Int32, l_f64 Float64)
    ENGINE = MergeTree ORDER BY tuple();

    CREATE TABLE gpu_join_right (k_u8 UInt8, k_u16 UInt16, k_u32 UInt32, k_u64 UInt64,
                                 k_i8 Int8, k_i16 Int16, k_i32 Int32, k_i64 Int64,
                                 k_dup UInt32, k_none UInt32, k_second UInt32,
                                 k_f64 Float64, k_str String, k_null Nullable(UInt32),
                                 k_lc LowCardinality(String),
                                 r_u64 UInt64, r_i32 Int32, r_f32 Float32, r_f64 Float64)
    ENGINE = MergeTree ORDER BY tuple();

    -- The two sides' keys are the same expressions of \`number\` over different row counts, so every
    -- width has some keys that match and some that do not. \`k_dup\` takes few values on both sides,
    -- which is what makes one probe row match several build rows and the output larger than the
    -- block that produced it. \`k_none\` is shifted on the right by more than either table's range,
    -- so a join on it matches nothing.
    --
    -- The float payloads hold multiples of an eighth. A payload is only ever copied - gathered on
    -- the device, not computed - so any value would come back bit for bit, but exactly
    -- representable ones keep the comparison about the join rather than about formatting.
    INSERT INTO gpu_join_left
    SELECT number % 251, number % 1000, number % 997, number % 503,
           number % 127 - 63, number % 999 - 500, number % 601 - 300, number % 701 - 350,
           number % 50, number, number % 3,
           number % 9, toString(number % 5), if(number % 11 = 0, NULL, number % 13), toString(number % 7),
           number, -number, number / 8
    FROM numbers(1000);

    INSERT INTO gpu_join_right
    SELECT number % 251, number % 1000, number % 997, number % 503,
           number % 127 - 63, number % 999 - 500, number % 601 - 300, number % 701 - 350,
           number % 50, number + 500000, number % 3,
           number % 9, toString(number % 5), if(number % 11 = 0, NULL, number % 13), toString(number % 7),
           number * 3, -number * 3, number / 8, number / 4
    FROM numbers(200);
"

GPU_SETTINGS=()
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    # The algorithm throws on a machine that has no usable device rather than falling back, which is
    # the point of it - so ask for one join it can do and see whether this machine is such a one.
    if $CLICKHOUSE_CLIENT --join_algorithm gpu_hash \
        --query "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32" > /dev/null 2>&1; then
        GPU_SETTINGS=(--join_algorithm gpu_hash)
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

# A join `gpu_hash` cannot do has to fail rather than be handed to another algorithm. Asking for
# `gpu_hash` alone is what makes that visible: with a second algorithm in the list the query would
# simply run on that one, which is the next check.
function expect_ineligible()
{
    local query="$1"
    local message

    message=$($CLICKHOUSE_CLIENT --join_algorithm gpu_hash --query "$query" 2>&1 > /dev/null)

    if [ -z "$message" ]; then
        echo "NO ERROR for '$query'"
    # The first is `chooseJoinAlgorithm`'s own refusal, reached after every enabled algorithm has
    # declined the join. The second is the check it makes before the loop, for an `ON` section with
    # several disjuncts, which only `hash` can do at all.
    elif [[ "$message" == *"None of the algorithms enabled by the 'join_algorithm' setting"* ]] \
        || [[ "$message" == *"Only \`hash\` join supports multiple ORs for keys"* ]]; then
        echo "ok"
    else
        echo "UNEXPECTED ERROR for '$query': $(echo "$message" | head -1)"
    fi
}

# The same ineligible join with `hash` listed after `gpu_hash` has to fall through to `hash` and
# return what `hash` alone returns - a new value in the priority list must not change what the
# others do. Only for ineligible shapes: an eligible one with no device fails instead of falling
# back, which is the whole point of the previous check.
function compare_with_hash()
{
    local query="$1"
    local with_hash
    local with_gpu_first

    with_hash=$($CLICKHOUSE_CLIENT --join_algorithm hash --query "$query")
    with_gpu_first=$($CLICKHOUSE_CLIENT --join_algorithm gpu_hash,hash --query "$query")

    if [ "$with_hash" == "$with_gpu_first" ]; then
        echo "ok"
    else
        echo "MISMATCH for '$query'"
    fi
}

# Every comparison orders its rows. Neither path promises an order for a `JOIN` - the device's is
# whatever `cudf::hash_join` produced, the CPU's whatever its hash table iterated - so without an
# `ORDER BY` a difference in output would say nothing about which rows joined.

# One key of each supported width, matched against the same width on the other side.
compare_with_cpu "SELECT l.k_u8, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u8 = r.k_u8 ORDER BY l.k_u8, r.r_u64"
compare_with_cpu "SELECT l.k_u16, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u16 = r.k_u16 ORDER BY l.k_u16, r.r_u64"
compare_with_cpu "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
compare_with_cpu "SELECT l.k_u64, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u64 = r.k_u64 ORDER BY l.k_u64, r.r_u64"
compare_with_cpu "SELECT l.k_i8, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_i8 = r.k_i8 ORDER BY l.k_i8, r.r_u64"
compare_with_cpu "SELECT l.k_i16, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_i16 = r.k_i16 ORDER BY l.k_i16, r.r_u64"
compare_with_cpu "SELECT l.k_i32, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_i32 = r.k_i32 ORDER BY l.k_i32, r.r_u64"
compare_with_cpu "SELECT l.k_i64, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_i64 = r.k_i64 ORDER BY l.k_i64, r.r_u64"

# Several payload columns of the right table at once, a float among them, together with columns of
# the left table - which are the ones indexed on the host rather than gathered on the device.
compare_with_cpu "SELECT l.k_u32, l.l_u64, l.l_i32, l.l_f64, r.r_u64, r.r_i32, r.r_f32, r.r_f64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY 1, 2, 3, 4, 5, 6, 7, 8"

# Only the right table's columns, and only the left table's - the join still has to produce the
# block the rest of the query expects in either case.
compare_with_cpu "SELECT r.r_u64, r.r_f64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY r.r_u64, r.r_f64"
compare_with_cpu "SELECT l.l_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.l_u64"

# No column of the right table in the output at all, so the build side carries its keys and nothing
# else. A single number needs no `ORDER BY`.
compare_with_cpu "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32"

# The right table's key column in the output. It is not read back from the device: an `INNER JOIN`
# on equality makes a matched pair's two keys equal, so it is a copy of the left key column.
compare_with_cpu "SELECT l.k_u32, r.k_u32, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY 1, 2, 3"
compare_with_cpu "SELECT k_u32, r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r USING (k_u32) ORDER BY k_u32, r_u64"

# Duplicate keys on the build side: \`k_dup\` takes fifty values over two hundred right rows and a
# thousand left ones, so every probe row matches four build rows and the output is four times the
# block that produced it.
compare_with_cpu "SELECT l.k_dup, l.l_u64, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_dup = r.k_dup ORDER BY l.k_dup, l.l_u64, r.r_u64"

# A probe side with no matches, and a build side with no rows. The second one never reaches the
# device at all: an inner join whose right table is empty returns nothing whatever the left table
# holds, which is also what `alwaysReturnsEmptySet` tells the pipeline.
compare_with_cpu "SELECT l.k_none, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_none = r.k_none ORDER BY l.k_none, r.r_u64"
compare_with_cpu "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_none = r.k_none"
compare_with_cpu "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l INNER JOIN (SELECT k_u32, r_u64 FROM gpu_join_right WHERE 0) AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
compare_with_cpu "SELECT count() FROM gpu_join_left AS l INNER JOIN (SELECT k_u32 FROM gpu_join_right WHERE 0) AS r ON l.k_u32 = r.k_u32"

# Both sides empty, and a left side of no rows against a right side that has some.
compare_with_cpu "SELECT count() FROM (SELECT k_u32 FROM gpu_join_left WHERE 0) AS l INNER JOIN (SELECT k_u32 FROM gpu_join_right WHERE 0) AS r ON l.k_u32 = r.k_u32"
compare_with_cpu "SELECT count() FROM (SELECT k_u32 FROM gpu_join_left WHERE 0) AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32"

# A key type mismatch between the sides. The planner casts both keys to a common type before an
# algorithm is picked, so these stay eligible - and the device is then given one element type, which
# is the only way it can compare the two.
compare_with_cpu "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u64 ORDER BY l.k_u32, r.r_u64"
compare_with_cpu "SELECT l.k_u8, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u8 = r.k_i32 ORDER BY l.k_u8, r.r_u64"
compare_with_cpu "SELECT l.k_i16, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_i16 = r.k_u16 ORDER BY l.k_i16, r.r_u64"

# A key that is an expression rather than a column, and a join under a `GROUP BY`, so that the
# join's output feeds something other than the client.
compare_with_cpu "SELECT l.k_u32 % 17 AS m, count(), sum(r.r_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 GROUP BY m ORDER BY m"
compare_with_cpu "SELECT r.k_u32 + 1 AS m, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY m, r.r_u64"

# The left side arriving in several blocks, so that the hash table is probed more than once and the
# results of the probes are concatenated.
compare_with_cpu "SELECT l.k_dup, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_dup = r.k_dup ORDER BY l.k_dup, r.r_u64 SETTINGS max_block_size = 64"
compare_with_cpu "SELECT count() FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 SETTINGS max_block_size = 8, max_threads = 4"

# Not eligible, and so an error rather than a join computed some other way.
#
# The float key is among these, and for a reason worth spelling out: ClickHouse compares a `Float64`
# key by its eight bytes, so `0.0` and `-0.0` are different keys, while cuDF compares float keys
# with IEEE equality and makes them one. That would change which rows join, so anyone who makes
# float keys eligible has to deal with it rather than discover it.
expect_ineligible "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l LEFT JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
expect_ineligible "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l RIGHT JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
expect_ineligible "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l FULL JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
expect_ineligible "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l ANY INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
expect_ineligible "SELECT l.k_u32 FROM gpu_join_left AS l SEMI LEFT JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32"
expect_ineligible "SELECT l.k_str, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_str = r.k_str ORDER BY l.k_str, r.r_u64"
expect_ineligible "SELECT l.k_null, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_null = r.k_null ORDER BY l.k_null, r.r_u64"
expect_ineligible "SELECT l.k_lc, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_lc = r.k_lc ORDER BY l.k_lc, r.r_u64"
expect_ineligible "SELECT l.k_f64, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_f64 = r.k_f64 ORDER BY l.k_f64, r.r_u64"
expect_ineligible "SELECT k_u32, k_second, r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r USING (k_u32, k_second) ORDER BY k_u32, k_second, r_u64"
expect_ineligible "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 AND l.k_second = r.k_second ORDER BY l.k_u32, r.r_u64"
expect_ineligible "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 OR l.k_second = r.k_second ORDER BY l.k_u32, r.r_u64"
# A `String` column in the output with an eligible key: the left side's columns are indexed on the
# host, which a `String` column would survive perfectly well, but one eligibility rule for both
# sides is worth more here than two.
expect_ineligible "SELECT l.k_str, r.r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_str, r.r_u64"
expect_ineligible "SELECT l.k_u32, r.k_str FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.k_str"
expect_ineligible "SELECT l.k_u32, r.k_null FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.k_null"

# The same shapes with `hash` listed after `gpu_hash`: each has to fall through to `hash` and return
# exactly what `hash` alone returns.
compare_with_hash "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l LEFT JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
compare_with_hash "SELECT l.k_u32, r.r_u64 FROM gpu_join_left AS l ANY INNER JOIN gpu_join_right AS r ON l.k_u32 = r.k_u32 ORDER BY l.k_u32, r.r_u64"
compare_with_hash "SELECT l.k_str, count(), sum(r.r_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_str = r.k_str GROUP BY l.k_str ORDER BY l.k_str"
compare_with_hash "SELECT l.k_null, count(), sum(r.r_u64) FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r ON l.k_null = r.k_null GROUP BY l.k_null ORDER BY l.k_null"
compare_with_hash "SELECT k_u32, k_second, r_u64 FROM gpu_join_left AS l INNER JOIN gpu_join_right AS r USING (k_u32, k_second) ORDER BY k_u32, k_second, r_u64"

$CLICKHOUSE_CLIENT --query "DROP TABLE gpu_join_left; DROP TABLE gpu_join_right"
