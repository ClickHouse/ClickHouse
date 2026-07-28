#!/usr/bin/env bash
# Tags: no-ordinary-database, use-rocksdb, no-fasttest
# Regression test: the `max_insert_threads` write fan-out of a plain INSERT must not apply to
# overwrite-by-key engines (`IKeyValueEntity`: EmbeddedRocksDB, KeeperMap, Redis). For them the result
# of inserting several rows with equal keys depends on the order in which the rows are committed - the
# last row wins - and a single-stream insert resolves that deterministically by row order. With the
# fan-out each branch commits its own independent batch (a RocksDB write batch / SST ingest, a set of
# Keeper requests), so the surviving value would become timing-dependent, and two KeeperMap branches
# creating the same new key could even fail with NODEEXISTS. Such inserts must stay single-stream,
# both for a direct destination (also behind an Alias) and for the target of a dependent
# materialized view, independently of any deduplication setting.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kv_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kv_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kv_src"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kv_rocksdb"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS kv_keeper_map SYNC"

# `optimize_for_bulk_insert = 0` selects the plain `EmbeddedRocksDBSink` (a RocksDB write batch per
# chunk) instead of the SST-ingesting bulk sink, so that the last value of the input wins per chunk
# and the outcome does not depend on the bulk sink's own batching.
$CLICKHOUSE_CLIENT -q "CREATE TABLE kv_rocksdb (k UInt64, v UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k SETTINGS optimize_for_bulk_insert = 0"
$CLICKHOUSE_CLIENT -q "CREATE TABLE kv_alias ENGINE = Alias('kv_rocksdb')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE kv_src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW kv_mv TO kv_rocksdb AS SELECT k, v FROM kv_src"

# A direct INSERT into an overwrite-by-key engine stays single-stream ...
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO kv_rocksdb VALUES (1, 1)" | grep -c "EmbeddedRocksDB.*Sink"

# ... including behind an Alias, whose AliasSink runs a nested INSERT into the same engine ...
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO kv_alias VALUES (1, 1)" | grep -c "AliasSink"

# ... and when the engine is only the target of a dependent materialized view, even with
# `parallel_view_processing` explicitly enabled.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO kv_src VALUES (1, 1)" | grep -c "EmbeddedRocksDB.*Sink"

# The "last row wins" resolution of equal keys must stay deterministic: many small blocks with the
# same key, the last value of the input has to survive. A few dozen single-row blocks are enough to
# make a fan-out visible while keeping the test fast under sanitizers.
seq 1 40 | awk '{print 1 "\t" $1}' | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=1 --max_insert_block_size=1 --max_block_size=1 -q \
    "INSERT INTO kv_rocksdb FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM kv_rocksdb WHERE k = 1"

# The same through the dependent materialized view.
seq 1 40 | awk '{print 2 "\t" $1}' | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --parallel_view_processing=1 \
    --min_insert_block_size_rows=1 --max_insert_block_size=1 --max_block_size=1 -q \
    "INSERT INTO kv_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM kv_rocksdb WHERE k = 2"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM kv_src"

# A KeeperMap destination: two sink branches creating the same new key would race on the Keeper
# `create` request, so it stays single-stream as well.
$CLICKHOUSE_CLIENT -q "CREATE TABLE kv_keeper_map (k UInt64, v UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/04648') PRIMARY KEY k"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO kv_keeper_map VALUES (1, 1)" | grep -c "KeeperMapSink"

# The last value of the input has to win here too, deterministically.
seq 1 20 | awk '{print 1 "\t" $1}' | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 \
    --min_insert_block_size_rows=1 --max_insert_block_size=1 --max_block_size=1 -q \
    "INSERT INTO kv_keeper_map FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT k, v FROM kv_keeper_map WHERE k = 1"

# A plain MergeTree destination without deduplication still fans out - the guard is scoped to
# overwrite-by-key engines.
$CLICKHOUSE_CLIENT -q "DROP TABLE kv_mv"
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO kv_src VALUES (1, 1)" | grep -c "MergeTreeSink"

$CLICKHOUSE_CLIENT -q "DROP TABLE kv_keeper_map SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE kv_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE kv_rocksdb"
$CLICKHOUSE_CLIENT -q "DROP TABLE kv_src"
