#!/usr/bin/env bash
# Tags: long, no-fasttest, no-asan, no-tsan, no-ubsan
# Tag no-fasttest: needs remote() with multiple addresses
# Tags no-asan, no-tsan, no-ubsan: this test is specifically for MSan on ARM

# Reproducer for ARM MSan false positive in `ConnectionPoolWithFailover::getMany`.
#
# The error:
#   WARNING: MemorySanitizer: use-of-uninitialized-value
#     #0 vector<TryResult>::erase                 (vector.h:1172)
#     #1 erase_if                                 (erase.h:40)
#     #2 ConnectionPoolWithFailover::getManyImpl   (ConnectionPoolWithFailover.cpp:237)
#     #3 ConnectionPoolWithFailover::getMany       (ConnectionPoolWithFailover.cpp:135)
#     #4 RemoteQueryExecutor::$_0::operator()      (RemoteQueryExecutor.cpp:255)
#     #5 function::operator()                      (function.h:508)
#     #6 RemoteQueryExecutorReadContext::Task::run  (RemoteQueryExecutorReadContext.cpp:49)
#     #7 AsyncTaskExecutor::Routine::operator()     (AsyncTaskExecutor.cpp:89)
#     #8 Fiber::RoutineImpl::operator()             (Fiber.h:75)
#     #9 fiber_entry_func                           (fiber_ucontext.hpp:81)
#
# The root cause: on the fiber's heap-allocated stack, compiler-generated
# temporaries from inlined functions (e.g. `getLogger` returning `shared_ptr`)
# leave dirty MSan shadow from struct padding. This shadow persists across
# function calls on the fiber stack and contaminates later stack allocations,
# such as the `vector<TryResult>` in `getMany`.
#
# The test runs concurrent distributed queries via `remote()` which go through
# `RemoteQueryExecutor` → `ConnectionPoolWithFailover` on fibers.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TIMELIMIT=$((SECONDS + 120))

# The error requires diverse code paths on fibers to create varied stack layouts.
# Mix distributed queries with different shapes, settings, and error paths
# to maximize the chance of the fiber stack shadow contamination.
# Queries to unavailable replicas trigger the ConnectionPoolWithFailover retry
# and erase_if logic that is the actual site of the MSan false positive.

# Distributed queries on fibers — the actual trigger site
run_distributed()
{
    local TL=$1
    while [ $SECONDS -lt "$TL" ]; do
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.{1,2}', numbers(100)) FORMAT Null"
        ${CLICKHOUSE_CLIENT} -q "SELECT sum(number) FROM remote('127.0.0.{1,2}', numbers(500)) GROUP BY number % 10 FORMAT Null"
        ${CLICKHOUSE_CLIENT} -q "SELECT number % 3, sum(number) FROM remote('127.0.0.{1,2}', numbers(100)) GROUP BY ROLLUP(number % 3) SETTINGS group_by_use_nulls=1 FORMAT Null"
        ${CLICKHOUSE_CLIENT} -q "SELECT uniq(number) FROM remote('127.0.0.{1,2}', numbers(1000)) FORMAT Null"
        ${CLICKHOUSE_CLIENT} -q "SELECT * FROM remote('127.0.0.{1,2}', numbers(10)) ORDER BY number FORMAT Null"
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.{1,2}', system.one) FORMAT Null"
        ${CLICKHOUSE_CLIENT} -q "SELECT 1 FROM remote('127.0.0.{1,2}', system.nonexistent)" 2>/dev/null
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.{1,2}', numbers(100)) WHERE number IN (SELECT number FROM numbers(50)) FORMAT Null"
    done
}

# Diverse table operations — creates varied stack layouts and generates logging
run_table_ops()
{
    local TL=$1
    local ID=0
    while [ $SECONDS -lt "$TL" ]; do
        ID=$((ID + 1))
        ${CLICKHOUSE_CLIENT} -q "
            DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_msan_$ID;
            CREATE TABLE ${CLICKHOUSE_DATABASE}.t_msan_$ID (id UInt64, v String DEFAULT '', e UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY id;
            INSERT INTO ${CLICKHOUSE_DATABASE}.t_msan_$ID SELECT number, toString(number), 0 FROM numbers(1000);
            ALTER TABLE ${CLICKHOUSE_DATABASE}.t_msan_$ID UPDATE e = id WHERE 1;
            SELECT count() FROM remote('127.0.0.{1,2}', ${CLICKHOUSE_DATABASE}.t_msan_$ID) FORMAT Null;
            DROP TABLE ${CLICKHOUSE_DATABASE}.t_msan_$ID;
        " 2>/dev/null
    done
}

for _ in $(seq 1 12); do run_distributed "$TIMELIMIT" & done
for _ in $(seq 1 4); do run_table_ops "$TIMELIMIT" & done

wait

echo "OK"
