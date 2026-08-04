#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers, long
# Tag no-fasttest: needs the Parquet output format, which the fast-test build omits.
# Tag no-sanitizers: ~67 clickhouse-local starts; under MSan the median run took 512 s of the
# 600 s per-test cap and the tail got killed. The hang being guarded is not sanitizer-specific.
# Tag long: many serial writes, and the flaky check runs many copies of a test at once.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The thread allocation fault injector is process global, so this runs in `clickhouse local`:
# raising its probability here cannot disturb the server or a concurrent copy of this test.
WORK="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}_04690"
rm -rf "$WORK"
mkdir -p "$WORK"
trap 'cd / && rm -rf "$WORK"' EXIT

# `file()` in `clickhouse local` resolves a relative path against the current directory, not
# against `--path`, so run from the scratch directory to keep the output out of the source tree.
cd "$WORK" || exit 1

WRITE="INSERT INTO FUNCTION file('04690.parquet', Parquet, 'a UInt64, b UInt64')
       SELECT number, sleepEachRow(0.02) FROM numbers(20)
       SETTINGS output_format_parquet_row_group_size = 1, engine_file_truncate_on_insert = 1;"

SETUP="SET output_format_parquet_parallel_encoding = 1;
       SET max_threads = 4;
       SET max_block_size = 1;
       SET function_sleep_max_microseconds_per_block = 100000000;"

# Options after an empty `--` bypass the unrecognized-option check and unknown keys are dropped
# silently, so a typo or a rename would leave the injector off with exit status 0. One spelling,
# shared by every invocation below and asserted live, is what keeps that failure loud.
inject() { echo "--cannot_allocate_thread_fault_injection_probability=$1"; }

# Positive control: with nothing injected the same write must complete, take the parallel encoder
# path, and leave readable multi-row-group data. Without it a run that never reached the encoder
# would be indistinguishable from a successful write. Under timeout like every other write here:
# trySchedule can fail for real on queue saturation (zero-wait path), so a regression can wedge
# this write too, and it must surface as HUNG rather than stall the whole shard.
timeout 120 ${CLICKHOUSE_LOCAL} --path "$WORK/db" -q "
    $SETUP
    SELECT getSetting('output_format_parquet_parallel_encoding'), getSetting('max_threads') > 1;
    $WRITE
    SELECT count(), sum(a) FROM file('04690.parquet', Parquet);
    SELECT num_row_groups > 1 FROM file('04690.parquet', ParquetMetadata);
"
rc=$?
rm -rf "$WORK/db"
if [ "$rc" -eq 124 ]
then
    echo "HUNG"
    exit 1
elif [ "$rc" -ne 0 ]
then
    echo "CONTROL FAILED rc=$rc"
    exit 1
fi

# `changed` is what separates an explicitly set value from the default, so this asserts the exact
# spelling really reaches the injector rather than being ignored. No write happens here, but under
# timeout anyway so no invocation in this file can wedge the shard.
timeout 120 ${CLICKHOUSE_LOCAL} --path "$WORK/db" -q "
    SELECT countIf(toFloat64(value) > 0 AND changed)
    FROM system.server_settings
    WHERE name = 'cannot_allocate_thread_fault_injection_probability';
" -- "$(inject 0.05)"
rm -rf "$WORK/db"

# At probability 1 the very first thread the encoder asks for is refused, and only the injector
# words a refusal this way, so this is a deterministic check that it is armed and drawing. It is
# what proves the loop below injects at all; the loop itself cannot show this, because a refused
# first thread is reported by an exception rather than by this text.
timeout 120 ${CLICKHOUSE_LOCAL} --path "$WORK/db" -q "
    $SETUP
    SYSTEM START THREAD FUZZER;
    $WRITE
" -- "$(inject 1)" >/dev/null 2>"$WORK/err.txt"
rm -rf "$WORK/db"
if ! grep -q "fault injected" "$WORK/err.txt"
then
    echo "INJECTOR NEVER FIRED"
    cat "$WORK/err.txt"
    exit 1
fi

# `timeout` is the oracle: completion is asserted, the hang is never asserted. The hit rate is not
# monotonic in the probability, because raising it also raises the chance of refusing the very
# first thread and ending the write early, and the peak sits elsewhere on every platform. Hence a
# ladder rather than one pinned value: repeated rungs where the peak was measured here, small ones
# to keep something completing where threads drain more slowly.
completed=0
for p in 0.05 0.05 0.03 0.03 0.02 0.01 0.005 0.002
do
    for _ in {1..8}
    do
        # The budget is only ever spent on a real hang, so it is sized for margin: a completing
        # write stays under 2s even with 50 copies of this test running at once.
        timeout 120 ${CLICKHOUSE_LOCAL} --path "$WORK/db" -q "
            $SETUP
            SYSTEM START THREAD FUZZER;
            $WRITE
        " -- "$(inject $p)" >/dev/null 2>"$WORK/err.txt"
        rc=$?

        rm -rf "$WORK/db"

        # 124 is `timeout`'s own status, i.e. the write never returned. A CANNOT_SCHEDULE_TASK is
        # the correct outcome of an allocation that genuinely failed; the shell truncates its 439
        # to 183, so match on the message. Anything else must not pass silently.
        if [ "$rc" -eq 124 ]
        then
            echo "HUNG"
            exit 1
        elif [ "$rc" -eq 0 ]
        then
            completed=$((completed + 1))
        elif ! grep -q "CANNOT_SCHEDULE_TASK" "$WORK/err.txt"
        then
            echo "UNEXPECTED rc=$rc"
            cat "$WORK/err.txt"
            exit 1
        fi
    done
done

# The hang needs a write that gets past its first schedule, so a run where every iteration was
# refused early would assert nothing.
if [ "$completed" -eq 0 ]
then
    echo "NO INJECTED ITERATION COMPLETED completed=$completed"
    exit 1
fi

echo "OK"
