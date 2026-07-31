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
    DROP TABLE IF EXISTS tt_alias_1;
    DROP TABLE IF EXISTS tt_alias_2;
    DROP TABLE IF EXISTS rdb_buf;
    DROP TABLE IF EXISTS tt_rdb;
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

# Wait for the reader to actually be running, so the lock is really held when we truncate.
for _ in {1..200}; do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$READER_ID'") -gt 0 ]] && break
    sleep 0.05
done

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 3" 2>&1 \
    | grep -c -m1 "DEADLOCK_AVOIDED" | sed 's/^/truncate blocked by reader\t/'
wait

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb_alias SETTINGS lock_acquire_timeout = 3" \
    && echo -e "truncate after reader\t1"

# TRUNCATE TABLES ... LIKE truncates every matched table concurrently on a thread pool, sharing one
# query context, so several tasks can want the same target lock at once: two aliases over one target,
# or an alias next to the target itself. A separate set of tables keeps the pattern off rdb_buf,
# because Buffer is matched by the loop but does not implement TRUNCATE.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE tt_rdb (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
    CREATE TABLE tt_alias_1 ENGINE = Alias($CLICKHOUSE_DATABASE, 'tt_rdb');
    CREATE TABLE tt_alias_2 ENGINE = Alias($CLICKHOUSE_DATABASE, 'tt_rdb');
    INSERT INTO tt_rdb SELECT number, repeat('x', 200) FROM numbers(1000);
"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLES FROM $CLICKHOUSE_DATABASE LIKE 'tt\_alias%'"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after truncate two aliases', count() FROM tt_rdb"

$CLICKHOUSE_CLIENT -q "INSERT INTO tt_rdb SELECT number, repeat('x', 200) FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLES FROM $CLICKHOUSE_DATABASE LIKE 'tt\_%'"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after truncate aliases with target', count() FROM tt_rdb"

# The target is still usable through every route, so neither the storage nor its handle was lost.
$CLICKHOUSE_CLIENT -q "
    TRUNCATE TABLE rdb_alias;
    SELECT 'rows after truncate', count() FROM rdb;
    INSERT INTO rdb VALUES (1, 'a');
    SELECT 'direct', count() FROM rdb;
    SELECT 'through alias', count() FROM rdb_alias;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE rdb_alias;
    DROP TABLE tt_alias_1;
    DROP TABLE tt_alias_2;
    DROP TABLE rdb_buf;
    DROP TABLE tt_rdb;
    DROP TABLE rdb;
"
