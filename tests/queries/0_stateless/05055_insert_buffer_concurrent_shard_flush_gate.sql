-- The rows of one INSERT can end up in several shards of a `Buffer` table, and the shards are then
-- flushed in parallel. Every one of those flushes writes rows of that query, so all of them must
-- share the query's single `Too many parts` gate: otherwise the check of one flush counts the part
-- that a sibling flush of the same query has just written, and the query is rejected with
-- `TOO_MANY_PARTS` although it wrote nothing but its own parts.

DROP TABLE IF EXISTS t_buffer_concurrent_flush;
DROP TABLE IF EXISTS t_buffer_concurrent_flush_dst;

CREATE TABLE t_buffer_concurrent_flush_dst (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS parts_to_throw_insert = 1, parts_to_delay_insert = 1;

-- Two shards, so `flushAllBuffers` flushes them in parallel; the thresholds never fire on their own.
CREATE TABLE t_buffer_concurrent_flush (x UInt64) ENGINE = Buffer(currentDatabase(), t_buffer_concurrent_flush_dst, 2, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);

-- Single-row blocks, so the two rows are spread over the two shards.
INSERT INTO t_buffer_concurrent_flush SELECT number FROM numbers(2)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

OPTIMIZE TABLE t_buffer_concurrent_flush;

SELECT count() FROM t_buffer_concurrent_flush_dst;

DROP TABLE t_buffer_concurrent_flush;
DROP TABLE t_buffer_concurrent_flush_dst;
