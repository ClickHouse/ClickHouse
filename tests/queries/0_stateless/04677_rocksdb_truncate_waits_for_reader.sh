#!/usr/bin/env bash
# Tags: long, no-ordinary-database, no-fasttest, use-rocksdb
# Tag long: the readers have to outlast the truncates that must wait for them, so the runtime is a
# floor set by the assertions
# Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

# TRUNCATE of an EmbeddedRocksDB table used to close and free its rocksdb handle while a full scan
# was still iterating it, which AddressSanitizer reports as a heap-use-after-free inside the rocksdb
# iterator. TRUNCATE now waits for such a scan instead, so both routes below must let the scan finish
# and still truncate. Reaching the target through Buffer is the interesting route: it bypasses the
# alias, which is why locking the alias never covered it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS rdb_alias;
    DROP TABLE IF EXISTS rdb_buf;
    DROP TABLE IF EXISTS rdb;

    CREATE TABLE rdb (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
    INSERT INTO rdb SELECT number, repeat('x', 200) FROM numbers(300000);
    CREATE TABLE rdb_buf AS rdb ENGINE = Buffer($CLICKHOUSE_DATABASE, 'rdb', 1, 1, 1, 1, 1, 1, 1);
    CREATE TABLE rdb_alias ENGINE = Alias($CLICKHOUSE_DATABASE, 'rdb');
"

# One row per block keeps the iterator being pulled for the whole window, and the window is a sleep
# over a bounded row count rather than a scan of the whole table, so it costs the same on every build
# flavour. Both timeouts are pinned rather than inherited: the CI config caps a wait at 60s, and the
# runner fails any test that writes to stderr.
READ_SETTINGS="max_threads = 1, max_block_size = 1, lock_acquire_timeout = 300"
# Every count() below is compared against an exact literal, and the runner randomizes this setting,
# under which the engine answers from rocksdb's key estimate instead. A low estimate would let a
# build that destroyed the data pass the timeout arm's data-preservation assertion.
EXACT_COUNT="optimize_trivial_approximate_count_query = 0"

# $1 = table the scans read, $2 = table the truncate goes through, $3 = row label
run_concurrent_scans_then_truncate() {
    local reader_table="$1" truncate_table="$2" label="$3"
    local seen="" reached=0
    local pids=() outs=()

    # sleepEachRow returns 0 for every row, so the sum is exactly the number of rows the scan read:
    # a value, unlike a discarded stdout, that witnesses the scan running to completion.
    for j in 1 2 3; do
        local out="$CLICKHOUSE_TMP/scan_${label}_${j}_$CLICKHOUSE_DATABASE.out"
        rm -f "$out"
        $CLICKHOUSE_CLIENT --query_id="scan_${label}_${j}_$CLICKHOUSE_DATABASE" \
            -q "SELECT sum(sleepEachRow(0.1) + 1) FROM (SELECT k FROM $reader_table LIMIT 200) SETTINGS $READ_SETTINGS" > "$out" &
        pids+=($!)
        outs+=("$out")
    done

    # Accumulated across polls rather than read at one instant, and asserted: a scan that already
    # finished has also reached the target, while a scan that never got there would leave the truncate
    # uncontended and this cell would race nothing.
    for _ in {1..600}; do
        for id in $($CLICKHOUSE_CLIENT -q "
            SELECT query_id FROM system.processes
            WHERE query_id LIKE 'scan\_${label}\_%\_$CLICKHOUSE_DATABASE' AND read_rows > 0"); do
            if [[ " $seen " != *" $id "* ]]; then
                seen="$seen $id"
                reached=$((reached + 1))
            fi
        done
        [[ $reached -ge 3 ]] && break
        sleep 0.05
    done
    echo -e "$label scans reached target\t$reached"

    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE $truncate_table SETTINGS lock_acquire_timeout = 300"
    echo -e "$label truncate succeeded\t$(($? == 0 ? 1 : 0))"

    # Waiting for the lease is only half the contract: a build that lets the truncate through but
    # breaks the scan mid-iteration still empties the table, so the readers' own outcome is asserted.
    local finished=0 read_all=0 i
    for i in "${!pids[@]}"; do
        wait "${pids[$i]}" && finished=$((finished + 1))
        [[ $(cat "${outs[$i]}") == 200 ]] && read_all=$((read_all + 1))
        rm -f "${outs[$i]}"
    done

    echo -e "$label rows after truncate\t$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb SETTINGS $EXACT_COUNT")"
    echo -e "$label readers finished\t$finished"
    echo -e "$label readers read all rows\t$read_all"
    $CLICKHOUSE_CLIENT -q "INSERT INTO rdb SELECT number, repeat('x', 200) FROM numbers(300000)"
}

# Through the alias, which is the route the reported failure took.
run_concurrent_scans_then_truncate rdb_buf rdb_alias buffer_reader_alias_truncate

# And directly, because the lifetime bug never depended on an alias being involved.
run_concurrent_scans_then_truncate rdb rdb direct

# The wait is bounded, so a scan the truncate cannot outwait is reported instead of hanging. The row
# count afterwards is the point: a timed out TRUNCATE must leave the table exactly as it was, not
# empty it, so asserting only the error code would pass on a build that destroyed the data.
READER_ID="reader_$CLICKHOUSE_DATABASE"
# Killed rather than awaited, so a window far longer than needed costs no wall-clock. By pid, never by
# job spec: the scans above already consumed job numbers.
$CLICKHOUSE_CLIENT --query_id="$READER_ID" -q "
    SELECT sum(sleepEachRow(0.2)) FROM (SELECT k FROM rdb LIMIT 150) SETTINGS max_block_size = 1, max_threads = 1
" > /dev/null &
reader_pid=$!

# read_rows > 0 rather than mere presence in system.processes: the ProcessList entry is published
# before the pipeline is built, hence before the scan holds anything.
reader_started=0
for _ in {1..200}; do
    if [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$READER_ID' AND read_rows > 0") -gt 0 ]]; then
        reader_started=1
        break
    fi
    sleep 0.05
done
echo -e "reader started\t$reader_started"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 3" 2>&1 \
    | grep -c -m1 "TIMEOUT_EXCEEDED" | sed 's/^/truncate timed out on reader\t/'
echo -e "rows kept after timeout\t$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb SETTINGS $EXACT_COUNT")"
kill "$reader_pid" 2>/dev/null
wait "$reader_pid" 2>/dev/null

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 30" \
    && echo -e "truncate after reader\t1"

# A zero timeout means no timeout, as it does everywhere else the setting is read, so this TRUNCATE
# must outwait the reader rather than give up on it at once. The reader is bounded, so a build that
# regressed to waiting forever fails here instead of hanging until the runner's budget runs out.
# Through the alias, which is the only route that reaches this wait with a reader still holding the
# target: truncating directly takes an exclusive lock on it first, and that lock reads the same
# setting, so it absorbs the whole wait and nothing about zero would be under test.
$CLICKHOUSE_CLIENT -q "INSERT INTO rdb SELECT number, repeat('x', 200) FROM numbers(1000)"
ZERO_READER_ID="zero_reader_$CLICKHOUSE_DATABASE"
zero_out="$CLICKHOUSE_TMP/zero_reader_$CLICKHOUSE_DATABASE.out"
rm -f "$zero_out"
$CLICKHOUSE_CLIENT --query_id="$ZERO_READER_ID" -q "
    SELECT sum(sleepEachRow(0.1) + 1) FROM (SELECT k FROM rdb LIMIT 100) SETTINGS max_block_size = 1, max_threads = 1
" > "$zero_out" &
zero_reader_pid=$!

zero_reader_started=0
for _ in {1..200}; do
    if [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$ZERO_READER_ID' AND read_rows > 0") -gt 0 ]]; then
        zero_reader_started=1
        break
    fi
    sleep 0.05
done
echo -e "zero timeout reader started\t$zero_reader_started"

zero_start=$SECONDS
zero_err=$($CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 0" 2>&1)
zero_rc=$?
zero_elapsed=$((SECONDS - zero_start))
echo -e "zero timeout truncate timed out\t$(grep -c "TIMEOUT_EXCEEDED" <<< "$zero_err")"
echo -e "zero timeout truncate succeeded\t$((zero_rc == 0 ? 1 : 0))"
# A boolean, not the duration: the reader's window is 10s, so a truncate that waited for it cannot
# come back in under 2s, while the duration itself is not reference stable. A build that gives up at
# once returns in well under a second, so the two outcomes are not adjacent.
echo -e "zero timeout truncate waited\t$((zero_elapsed >= 2 ? 1 : 0))"
zero_finished=0
wait "$zero_reader_pid" && zero_finished=1
echo -e "zero timeout reader finished\t$zero_finished"
echo -e "zero timeout reader read all rows\t$([[ $(cat "$zero_out") == 100 ]] && echo 1 || echo 0)"
rm -f "$zero_out"
# Recovery, so the assertions below report on their own subject rather than on whether the arm above
# got as far as emptying the table. The reader is already awaited, so this cannot wait on anything.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb"

# What the wait must NOT do: block the handle's other users. The readers above never take
# rocksdb_ptr_mx, so on their own they cannot tell a wait that holds it from one that does not.
# A mutation does take it, shared, from inside its own read pipeline, so it also holds a lease
# the truncate is waiting for: waiting under the exclusive lock deadlocks the pair outright.
# sleepEachRow rather than a row count bounds the mutation's window by wall clock, so the
# window does not shrink on a faster build flavour.
$CLICKHOUSE_CLIENT -q "INSERT INTO rdb SELECT number, repeat('x', 200) FROM numbers(60)"
MUT_ID="mutation_$CLICKHOUSE_DATABASE"
# The predicate has to match: a mutation that deletes nothing never reaches the write that takes
# the mutex, and then there is no second lock to order against. What it deletes does not matter,
# the truncate below empties the table either way.
timeout 60 $CLICKHOUSE_CLIENT --query_id="$MUT_ID" -q "
    ALTER TABLE rdb DELETE WHERE v LIKE 'x%' AND sleepEachRow(0.1) = 0
    SETTINGS mutations_sync = 1, max_block_size = 1, max_threads = 1, lock_acquire_timeout = 300
" > /dev/null 2>&1 &
mutation_pid=$!

# read_rows above one block, so the mutation is inside its pull loop and already holds its lease
# rather than merely having been published to the ProcessList.
mutation_pulling=0
for _ in {1..400}; do
    if [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$MUT_ID' AND read_rows > 10") -gt 0 ]]; then
        mutation_pulling=1
        break
    fi
    sleep 0.05
done
echo -e "mutation pulling\t$mutation_pulling"

# Through the alias, for the reason given above: a direct TRUNCATE takes the target's exclusive
# table lock first and would absorb the wait. The timeout is bounded rather than zero, and it is what
# makes the cell report: waiting under the exclusive lock cannot be outwaited, because the mutation
# needs that same lock to make the progress the wait is waiting for, so the truncate reaches its
# deadline and this cell turns 0. A zero timeout there would instead hang until the runner gives up,
# and would leave both queries wedged for the rest of the file.
timeout 60 $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 30" > /dev/null 2>&1
echo -e "truncate completed beside mutation\t$(($? == 0 ? 1 : 0))"
wait "$mutation_pid"
echo -e "mutation completed beside truncate\t$(($? == 0 ? 1 : 0))"

# The target is still usable through every route, so neither the storage nor its handle was lost.
# INSERT ... SELECT rather than INSERT ... VALUES: the runner redirects only stdout and stderr, so
# the client inherits the runner's stdin and a VALUES insert blocks on it until the test times out.
$CLICKHOUSE_CLIENT -q "
    SELECT 'rows after truncate', count() FROM rdb SETTINGS $EXACT_COUNT;
    INSERT INTO rdb SELECT 1, 'a';
    SELECT 'direct', count() FROM rdb SETTINGS $EXACT_COUNT;
    SELECT 'through alias', count() FROM rdb_alias SETTINGS $EXACT_COUNT;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rdb_alias;
    DROP TABLE rdb_buf;
    DROP TABLE rdb;
"
