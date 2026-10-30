#!/usr/bin/env bash
# An `INSERT` through two `Alias` hops: an `Alias` can point to a table that is later replaced by another
# `Alias`. Every branch of the outer `max_insert_threads` fan-out runs a nested `INSERT` into the second
# `Alias`, which runs a nested `INSERT` into the final table with its dependent materialized view. The
# nested `INSERT`s stay single-stream at every hop, and the squashing stages of all the copies share the
# memory budget of the squashing (the number of copies is carried through both hops, see
# `InterpreterInsertQuery::setOneOfConcurrentCopies`). Every row has to reach both the final table and
# the view target.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS alias_2h_outer;
DROP TABLE IF EXISTS alias_2h_inner;
DROP TABLE IF EXISTS alias_2h_mv;
DROP TABLE IF EXISTS alias_2h_src;
DROP TABLE IF EXISTS alias_2h_dst;

CREATE TABLE alias_2h_src (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0;
CREATE TABLE alias_2h_dst (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 0;
CREATE MATERIALIZED VIEW alias_2h_mv TO alias_2h_dst AS SELECT x FROM alias_2h_src;

-- An Alias cannot be created over another Alias, but its target can be replaced by one afterwards.
CREATE TABLE alias_2h_inner (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE alias_2h_outer ENGINE = Alias('alias_2h_inner');
DROP TABLE alias_2h_inner;
CREATE TABLE alias_2h_inner ENGINE = Alias('alias_2h_src');
"

seq 1 400 | $CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=1 \
    --min_insert_block_size_rows=100 --max_insert_block_size=100 --max_block_size=100 -q \
    "INSERT INTO alias_2h_outer FORMAT TSV"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM alias_2h_src), (SELECT sum(x) FROM alias_2h_src), (SELECT count() FROM alias_2h_dst), (SELECT sum(x) FROM alias_2h_dst)"

$CLICKHOUSE_CLIENT $SETTINGS --max_insert_threads=4 --parallel_view_processing=1 \
    -q "INSERT INTO alias_2h_outer SELECT number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SELECT (SELECT count() FROM alias_2h_src), (SELECT count() FROM alias_2h_dst)"

$CLICKHOUSE_CLIENT -q "
DROP TABLE alias_2h_outer;
DROP TABLE alias_2h_inner;
DROP TABLE alias_2h_mv;
DROP TABLE alias_2h_src;
DROP TABLE alias_2h_dst;
"
