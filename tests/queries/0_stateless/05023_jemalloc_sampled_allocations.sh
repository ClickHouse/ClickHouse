#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-msan, no-ubsan, no-fasttest, no-debug, no-llvm-coverage, no-parallel
#       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# NOTE: jemalloc is disabled under sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Server: sampled allocations of a profiled query are visible and well-formed.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_sampled (s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --jemalloc_enable_profiler=1 -q "INSERT INTO t_sampled SELECT repeat(repeat('a', 100), 1000000)"

${CLICKHOUSE_CLIENT} -q "
    SELECT
        count() > 0,
        countIf(size > usize) = 0,
        countIf(weight < 1) = 0,
        countIf(empty(trace)) = 0,
        min(sample_interval) > 0 AND min(sample_interval) = max(sample_interval)
    FROM system.jemalloc_sampled_allocations
"

# The collapsed heap profile must still parse while live sampled allocations
# add fragmentation records to the dump.
${CLICKHOUSE_CLIENT} -q "
    SELECT count() > 0 FROM system.jemalloc_profile_text
    SETTINGS jemalloc_profile_text_output_format = 'collapsed', jemalloc_profile_text_symbolize_with_inline = 0
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_sampled SYNC"

# Local (no interference from parallel tests): rows of an allocation disappear
# once it is freed. The 100MB string held by the Memory table is the only
# allocation of that magnitude in a fresh clickhouse-local process.
${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_sampled (s String) ENGINE = Memory;
    SET jemalloc_enable_profiler = 1;
    INSERT INTO t_sampled SELECT repeat(repeat('a', 100), 1000000);
    SELECT count() >= 1 FROM system.jemalloc_sampled_allocations WHERE usize >= 100000000;
    DROP TABLE t_sampled SYNC;
    SELECT count() = 0 FROM system.jemalloc_sampled_allocations WHERE usize >= 100000000;
"
