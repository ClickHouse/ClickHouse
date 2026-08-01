#!/usr/bin/env bash
# Tags: no-ordinary-database, no-fasttest, use-rocksdb
# Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

# TRUNCATE through an Alias used to take the exclusive lock on the alias only, never on the target
# whose data it destroys. A reader that reached the target without going through the alias (here via
# Buffer) therefore kept scanning an EmbeddedRocksDB handle that TRUNCATE had already closed and
# freed, which AddressSanitizer reports as a heap-use-after-free inside the rocksdb iterator.
# Reading and truncating both through the alias, or both directly, was already serialized.

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

# Scans that outlive the TRUNCATE they race with. A small max_block_size keeps the iterator alive
# for the whole window; every SELECT must still finish without an error.
for _ in {1..3}; do
    for _ in {1..3}; do
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb_buf SETTINGS max_threads = 1, max_block_size = 100" > /dev/null &
    done
    sleep 0.15
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias"
    wait
    $CLICKHOUSE_CLIENT -q "INSERT INTO rdb SELECT number, repeat('x', 200) FROM numbers(300000)"
done

# The lock itself, asserted deterministically rather than by timing: while a reader provably holds
# the target's share lock, TRUNCATE through the alias must wait for it and report DEADLOCK_AVOIDED,
# and must succeed once that reader is gone. This holds on every build flavour, not just sanitizers.
READER_ID="reader_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id="$READER_ID" -q "
    SELECT sum(sleepEachRow(0.2)) FROM (SELECT k FROM rdb LIMIT 25) SETTINGS max_block_size = 1, max_threads = 1
" > /dev/null &

# Wait until the reader has actually started scanning, so the target's share lock is really held.
# read_rows > 0 rather than mere presence in system.processes: the ProcessList entry is published
# before the interpreter is built, hence before any table lock is taken, so presence alone can be
# true while the lock does not exist yet.
# The outcome is asserted, not just waited for: on a timeout this loop would fall through with no
# reader holding the lock, and the assertions below would then measure an uncontended truncate.
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
    | grep -c -m1 "DEADLOCK_AVOIDED" | sed 's/^/truncate blocked by reader\t/'
wait

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 3" \
    && echo -e "truncate after reader\t1"

# MergeTree stays exempt from that lock, and the exemption is decided on the storage the catalog
# entry really wraps: with lazy_load_tables the catalog hands out a StorageProxy, which is not
# MergeTreeData, so testing the raw pointer would take the lock and hold it across the whole
# truncate. Here the same reader contention must NOT block the truncate.
LAZY_DB="${CLICKHOUSE_DATABASE}_lazy"
$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS $LAZY_DB SYNC;
    CREATE DATABASE $LAZY_DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE $LAZY_DB.mt (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
    INSERT INTO $LAZY_DB.mt SELECT number FROM numbers(100);
"
# Reload so the table is unloaded again and the catalog really returns the proxy, not the table.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE $LAZY_DB SYNC"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE $LAZY_DB"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $LAZY_DB.mt_alias ENGINE = Alias($LAZY_DB, 'mt')"
$CLICKHOUSE_CLIENT -q "SELECT 'lazy target is a proxy', engine = 'TableProxy' FROM system.tables WHERE database = '$LAZY_DB' AND name = 'mt'"

LAZY_READER_ID="lazy_reader_$CLICKHOUSE_DATABASE"
# Only stdout is redirected, unlike a suppressed 2>&1: a reader that fails outright must surface,
# because the runner fails any test with non-empty stderr. Measured that the deliberate kill below
# and the following DROP DATABASE write nothing to stderr, so this cannot fail spuriously.
$CLICKHOUSE_CLIENT --query_id="$LAZY_READER_ID" -q "
    SELECT sum(sleepEachRow(0.2)) FROM $LAZY_DB.mt SETTINGS max_block_size = 1, max_threads = 1
" > /dev/null &

# Asserted for the same reason as the first cell, and it matters more here: this cell's headline
# assertion is that the truncate is NOT blocked, which a reader that never started would also
# satisfy. Without this row the whole lazy-proxy cell could pass having tested nothing.
lazy_reader_started=0
for _ in {1..200}; do
    if [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$LAZY_READER_ID' AND read_rows > 0") -gt 0 ]]; then
        lazy_reader_started=1
        break
    fi
    sleep 0.05
done
echo -e "lazy reader started\t$lazy_reader_started"

# Status and stderr captured separately rather than folded into one grep: `grep -c DEADLOCK_AVOIDED`
# reads 0 both when the truncate succeeds and when it fails for any unrelated reason, so on its own
# it cannot tell "not blocked" from "broken". The succeeded row closes that half.
lazy_err=$($CLICKHOUSE_CLIENT -q "TRUNCATE TABLE $LAZY_DB.mt_alias SETTINGS lock_acquire_timeout = 3" 2>&1)
lazy_rc=$?
echo -e "truncate lazy proxied MergeTree succeeded\t$((lazy_rc == 0 ? 1 : 0))"
echo -e "truncate lazy proxied MergeTree blocked\t$(echo "$lazy_err" | grep -c -m1 "DEADLOCK_AVOIDED")"
kill %1 2>/dev/null
wait 2>/dev/null
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $LAZY_DB SYNC"

# TRUNCATE TABLES ... LIKE, which can want the same target lock from several pool tasks at once,
# is covered by 04678_alias_truncate_tables_like_overlap: forcing that overlap needs a server-global
# failpoint, so it lives in its own no-parallel test and this one stays parallel-safe.

# The target is still usable through every route, so neither the storage nor its handle was lost.
# INSERT ... SELECT rather than INSERT ... VALUES: the runner redirects only stdout and stderr, so
# the client inherits the runner's stdin and a VALUES insert blocks on it until the test times out.
$CLICKHOUSE_CLIENT -q "
    TRUNCATE TABLE rdb_alias;
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
