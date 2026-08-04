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

# $1 = table the scans read, $2 = table the truncate goes through, $3 = row label
run_concurrent_scans_then_truncate() {
    local reader_table="$1" truncate_table="$2" label="$3"
    local seen="" reached=0

    for j in 1 2 3; do
        $CLICKHOUSE_CLIENT --query_id="scan_${label}_${j}_$CLICKHOUSE_DATABASE" \
            -q "SELECT sum(sleepEachRow(0.1)) FROM (SELECT k FROM $reader_table LIMIT 200) SETTINGS $READ_SETTINGS" > /dev/null &
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
    wait
    echo -e "$label rows after truncate\t$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb")"
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
echo -e "rows kept after timeout\t$($CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb")"
kill "$reader_pid" 2>/dev/null
wait "$reader_pid" 2>/dev/null

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 30" \
    && echo -e "truncate after reader\t1"

# The target is still usable through every route, so neither the storage nor its handle was lost.
# INSERT ... SELECT rather than INSERT ... VALUES: the runner redirects only stdout and stderr, so
# the client inherits the runner's stdin and a VALUES insert blocks on it until the test times out.
$CLICKHOUSE_CLIENT -q "
    SELECT 'rows after truncate', count() FROM rdb;
    INSERT INTO rdb SELECT 1, 'a';
    SELECT 'direct', count() FROM rdb;
    SELECT 'through alias', count() FROM rdb_alias;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rdb_alias;
    DROP TABLE rdb_buf;
    DROP TABLE rdb;
"
