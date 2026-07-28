#!/usr/bin/env bash
# Regression test: the `max_insert_threads` write fan-out of a plain INSERT must not apply to a
# `Memory` table with the circular-buffer bounds `max_rows_to_keep` / `max_bytes_to_keep`.
# `MemorySink::onFinish` first evicts the oldest blocks to fit the bound and then appends its own
# batch, so the rows that survive are the ones committed last. A single-stream insert commits the
# input in order and always retains its tail; with the fan-out the retained rows would be whichever
# branch happens to commit last. Such inserts must stay single-stream, both for a direct destination
# (also behind an Alias) and for the target of a dependent materialized view.
# See https://github.com/ClickHouse/ClickHouse/pull/109000

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"
# One row per block, so that the retention loop evicts block by block and the retained window is
# exactly the tail of the input.
BLOCKS="--min_insert_block_size_rows=1 --max_insert_block_size=1 --max_block_size=1"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem_capped"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem_capped_mv_target"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS mem_src"

$CLICKHOUSE_CLIENT -q "CREATE TABLE mem_capped (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 10"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mem_capped_mv_target (x UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 10"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mem_plain (x UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mem_alias ENGINE = Alias('mem_capped')"
$CLICKHOUSE_CLIENT -q "CREATE TABLE mem_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0"
$CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW mem_mv TO mem_capped_mv_target AS SELECT x FROM mem_src"

# A direct INSERT into a capped `Memory` table stays single-stream ...
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO mem_capped VALUES (1)" | grep -c "MemorySink"

# ... including behind an Alias, whose AliasSink runs a nested INSERT into the same table ...
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO mem_alias VALUES (1)" | grep -c "AliasSink"

# ... and when the capped table is only the target of a dependent materialized view, even with
# `parallel_view_processing` explicitly enabled.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO mem_src VALUES (1)" | grep -c "MemorySink"

# The retained window must be the tail of the input. Fill the buffer with 8 single-row blocks first
# (the retention only evicts blocks that are already committed), then insert 5 more rows: the three
# oldest blocks are evicted and rows 4 .. 13 survive.
$CLICKHOUSE_CLIENT $SETTINGS $BLOCKS --max_insert_threads=1 -q \
    "INSERT INTO mem_capped SELECT number + 1 FROM numbers(8)"
seq 9 13 | $CLICKHOUSE_CLIENT $SETTINGS $BLOCKS --max_insert_threads=4 -q \
    "INSERT INTO mem_capped FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT min(x), max(x), count() FROM mem_capped"

# The same through the dependent materialized view.
$CLICKHOUSE_CLIENT $SETTINGS $BLOCKS --max_insert_threads=1 -q \
    "INSERT INTO mem_src SELECT number + 1 FROM numbers(8)"
seq 9 13 | $CLICKHOUSE_CLIENT $SETTINGS $BLOCKS --max_insert_threads=4 --parallel_view_processing=1 -q \
    "INSERT INTO mem_src FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT min(x), max(x), count() FROM mem_capped_mv_target"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM mem_src"

# A `Memory` table without retention bounds has no such ordering contract and still fans out -
# the guard is scoped to the circular-buffer configuration.
$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 -q \
    "EXPLAIN PIPELINE INSERT INTO mem_plain VALUES (1)" | grep -c "MemorySink"

$CLICKHOUSE_CLIENT -q "DROP TABLE mem_mv"
$CLICKHOUSE_CLIENT -q "DROP TABLE mem_alias"
$CLICKHOUSE_CLIENT -q "DROP TABLE mem_capped"
$CLICKHOUSE_CLIENT -q "DROP TABLE mem_capped_mv_target"
$CLICKHOUSE_CLIENT -q "DROP TABLE mem_plain"
$CLICKHOUSE_CLIENT -q "DROP TABLE mem_src"
