#!/usr/bin/env bash

# Checks that a `sum` answered from columns held in GPU device memory - `gpu_column_cache_size`
# together with `allow_experimental_gpu_aggregation` - returns what the CPU returns.
#
# Where the build has no GPU support, or the machine no usable device, the setting is not applied
# and both sides of every comparison run on the CPU - the checks hold trivially, and the plan
# assertions print what they would have asserted. That is what makes this test runnable
# everywhere: it becomes a real comparison exactly on the machines that can do one, and says
# nothing about the rest.
#
# Everything runs in `clickhouse-local` rather than against the server, for two reasons. The cache
# lives as long as the process and is shared by every query in it, so "the same query twice, the
# second one from the cache" has to be two queries of one process. And its size is a server
# setting, which a test can only choose by starting a process with a configuration of its own -
# which is also how the cases with a cache too small for the data and with no cache at all are
# reached.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/gpu_resident_column_sum"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

trap 'rm -rf "${WORK_DIR}"' EXIT

# The table and the rows every case below starts from: two parts, so that there is more than one
# per-part sum for the aggregation above the source to add up.
#
# The `Float64` column holds multiples of an eighth, so that every partial sum of it is exact in a
# double whatever order the values are added in - the device reduces a part as a tree and adds the
# parts' sums afterwards, the CPU adds every value in order, and only exactly representable sums
# let the two be compared for equality.
#
# `r` is a `ReplacingMergeTree`, for the `FINAL` case: `MergeTree` refuses `FINAL` outright.
SETUP="
    CREATE TABLE t (u64 UInt64, i64 Int64, f64 Float64, u32 UInt32, n Nullable(UInt64), k UInt8)
    ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO t SELECT number, -number, number / 8, number, if(number % 7 = 0, NULL, number), number % 4
    FROM numbers(100000);

    INSERT INTO t SELECT number + 100000, -number, number / 8, number, if(number % 7 = 0, NULL, number), number % 4
    FROM numbers(100000);

    CREATE TABLE r (key UInt64, u64 UInt64) ENGINE = ReplacingMergeTree ORDER BY key;

    INSERT INTO r SELECT number, number FROM numbers(100000);
    INSERT INTO r SELECT number, number * 2 FROM numbers(50000);
"

# One process, its own data directory, its own cache size.
function run_local()
{
    local cache_size="$1"
    local gpu="$2"
    local queries="$3"

    local dir
    dir=$(mktemp -d "${WORK_DIR}/run.XXXXXX")

    cat > "${dir}/config.xml" <<EOF
<clickhouse>
    <gpu_column_cache_size>${cache_size}</gpu_column_cache_size>
    <logger><level>none</level><console>false</console></logger>
</clickhouse>
EOF

    ${CLICKHOUSE_LOCAL} --config-file "${dir}/config.xml" --path "${dir}/db" \
        --allow_experimental_gpu_aggregation "${gpu}" --query "${SETUP} ${queries}"

    rm -rf "${dir}"
}

# Whether this machine can run the device path at all. Asked the way the setting itself answers it:
# in a build that has GPU support, a query that would use the device fails on a machine that has
# none usable.
GPU=0
if [ "$(${CLICKHOUSE_LOCAL} --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    if run_local 134217728 1 "SELECT sum(u64) FROM t" > /dev/null 2>&1; then
        GPU=1
    fi
fi

# Runs the same queries with the device path on and off and compares the two answers.
function compare_with_cpu()
{
    local name="$1"
    local cache_size="$2"
    local queries="$3"

    local on_cpu
    local on_gpu

    on_cpu=$(run_local "${cache_size}" 0 "${queries}")
    on_gpu=$(run_local "${cache_size}" "${GPU}" "${queries}")

    if [ "${on_cpu}" == "${on_gpu}" ]; then
        echo "${name} ok"
    else
        echo "${name} MISMATCH: [${on_cpu}] on the CPU, [${on_gpu}] on the GPU"
    fi
}

# Whether the plan of a query reads from the cache. Only asked where there is a device: without one
# the step is never in a plan, and there is nothing to assert - so the expectation is printed and
# the line holds trivially, the same way the comparisons above do.
function check_plan()
{
    local name="$1"
    local expected="$2"
    local cache_size="$3"
    local query="$4"

    if [ "${GPU}" == "0" ]; then
        echo "${name} ${expected}"
        return
    fi

    if run_local "${cache_size}" 1 "EXPLAIN ${query}" | grep -q "ReadFromGPUResidentColumns"; then
        echo "${name} yes"
    else
        echo "${name} no"
    fi
}

CACHE=134217728

# The same query twice: the second one is answered from device memory and has to say the same thing.
compare_with_cpu "twice" "${CACHE}" "SELECT sum(u64) FROM t; SELECT sum(u64) FROM t"

# Several columns at once, and then again from the cache.
compare_with_cpu "columns" "${CACHE}" "
    SELECT sum(u64), sum(i64), sum(f64) FROM t;
    SELECT sum(u64), sum(i64), sum(f64) FROM t;
    SELECT sum(f64) FROM t"

# A new part between two runs: its columns are read while the old parts' are already resident.
compare_with_cpu "insert" "${CACHE}" "
    SELECT sum(u64) FROM t;
    INSERT INTO t SELECT number + 1000000, -number, number / 8, number, NULL, 0 FROM numbers(50000);
    SELECT sum(u64) FROM t;
    SELECT sum(u64) FROM t"

# A merge between two runs: the cached parts are gone from the table and the merged part is a part
# nothing has been cached for. The old entries are neither stale nor used - they belong to parts
# that are no longer read.
compare_with_cpu "optimize" "${CACHE}" "
    SELECT sum(u64), sum(f64) FROM t;
    OPTIMIZE TABLE t FINAL;
    SELECT sum(u64), sum(f64) FROM t;
    SELECT sum(u64), sum(f64) FROM t"

# A cache too small to hold even one part's column: every part is summed on the device and let go,
# which has to be as correct as keeping it.
compare_with_cpu "tiny_cache" 1024 "SELECT sum(u64) FROM t; SELECT sum(u64) FROM t"

# No cache at all: the aggregation runs on the device the ordinary way, uploading its column.
compare_with_cpu "no_cache" 0 "SELECT sum(u64) FROM t; SELECT sum(u64) FROM t"

# The shapes the pass has to refuse. Each is still answered - on the CPU or by the ordinary device
# path - and has to give the CPU's answer.
compare_with_cpu "where" "${CACHE}" "SELECT sum(u64) FROM t WHERE k = 1"
compare_with_cpu "group_by" "${CACHE}" "SELECT k, sum(u64) FROM t GROUP BY k ORDER BY k"
compare_with_cpu "final" "${CACHE}" "SELECT sum(u64) FROM r FINAL"
compare_with_cpu "nullable" "${CACHE}" "SELECT sum(n) FROM t"
compare_with_cpu "narrow" "${CACHE}" "SELECT sum(u32) FROM t"
compare_with_cpu "expression" "${CACHE}" "SELECT sum(u64 + 1) FROM t"
# A computed argument that still reaches the device path as a plain `sum`: the expression between
# the aggregation and the read computes it, so the columns a part stores are not what is summed.
compare_with_cpu "computed" "${CACHE}" "SELECT sum(u64 % 7) FROM t"

# And that it fires exactly where it should.
check_plan "plan_plain" yes "${CACHE}" "SELECT sum(u64) FROM t"
check_plan "plan_columns" yes "${CACHE}" "SELECT sum(u64), sum(i64), sum(f64) FROM t"
check_plan "plan_where" no "${CACHE}" "SELECT sum(u64) FROM t WHERE k = 1"
check_plan "plan_group_by" no "${CACHE}" "SELECT k, sum(u64) FROM t GROUP BY k"
check_plan "plan_final" no "${CACHE}" "SELECT sum(u64) FROM r FINAL"
check_plan "plan_nullable" no "${CACHE}" "SELECT sum(n) FROM t"
check_plan "plan_narrow" no "${CACHE}" "SELECT sum(u32) FROM t"
check_plan "plan_expression" no "${CACHE}" "SELECT sum(u64 + 1) FROM t"
check_plan "plan_computed" no "${CACHE}" "SELECT sum(u64 % 7) FROM t"
check_plan "plan_no_cache" no 0 "SELECT sum(u64) FROM t"
