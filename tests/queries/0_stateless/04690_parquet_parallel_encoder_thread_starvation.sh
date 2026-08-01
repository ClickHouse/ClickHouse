#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the Parquet output format, which the fast-test build omits.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The parallel Parquet encoder counts its live threads in `threads_running`. A thread started
# through `trySchedule` was not counted, yet it decremented the count when it exited, so the count
# underflowed. After that `startMoreThreadsIfNeeded` never took the `scheduleOrThrowOnError`
# branch again, and a later `trySchedule` failure could leave a non-empty task queue with no
# thread to drain it, parking `finalizeImpl` on its condition variable forever.
#
# The thread allocation fault injector is process global, so this runs in `clickhouse local`:
# raising its probability here cannot disturb the server or a concurrent copy of this test.
# Each iteration reaches the starved state with probability ~0.3, so 30 of them make a miss
# very unlikely while keeping the whole test well under a minute.
#
# `timeout` is the oracle. Completion is asserted; the hang is never asserted.
WORK="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}_04690"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'cd / && rm -rf "$WORK"' EXIT

# `file()` in `clickhouse local` resolves a relative path against the current directory, not
# against `--path`, so run from the scratch directory to keep the output out of the source tree.
cd "$WORK" || exit 1

for _ in {1..30}
do
    timeout 30 ${CLICKHOUSE_LOCAL} --path "$WORK/db" -q "
        SET output_format_parquet_parallel_encoding = 1;
        SET max_threads = 4;
        SET max_block_size = 1;
        SET function_sleep_max_microseconds_per_block = 100000000;
        SYSTEM START THREAD FUZZER;
        INSERT INTO FUNCTION file('04690.parquet', Parquet, 'a UInt64, b UInt64')
        SELECT number, sleepEachRow(0.02) FROM numbers(20)
        SETTINGS output_format_parquet_row_group_size = 1, engine_file_truncate_on_insert = 1;
    " -- --cannot_allocate_thread_fault_injection_probability=0.08 >/dev/null 2>&1
    rc=$?

    rm -rf "$WORK/db"

    # 124 is `timeout`'s own exit status, i.e. the write never returned. Any other status means it
    # did return: a CANNOT_SCHEDULE_TASK from the injector is the correct outcome of a thread
    # allocation that genuinely failed, and is not what this test looks for.
    if [ "$rc" -eq 124 ]
    then
        echo "HUNG"
        exit 1
    fi
done

echo "OK"
