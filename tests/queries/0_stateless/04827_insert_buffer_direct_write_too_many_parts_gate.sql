-- The direct writes of an INSERT into a Buffer table run through nested INSERTs into the
-- destination table: one per block that bypasses the buffer, and one per flush the buffer runs by
-- threshold from within the query. The `Too many parts` check of those nested INSERTs must not
-- count the parts the same outer query has already committed on the destination: with
-- `parts_to_throw_insert = 1` the second direct write of a multi-block INSERT used to fail with
-- TOO_MANY_PARTS after the first one had already written a part.

DROP TABLE IF EXISTS t_04827_dst;
DROP TABLE IF EXISTS t_04827_buf_bypass;
DROP TABLE IF EXISTS t_04827_buf_flush;

CREATE TABLE t_04827_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- Every block exceeds the zero max_rows threshold and is written directly, skipping the buffer:
-- one nested INSERT per block.
CREATE TABLE t_04827_buf_bypass (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_04827_dst, 1, 1000000, 1000000, 1000000, 0, 1000000000, 1000000000);

INSERT INTO t_04827_buf_bypass SELECT number FROM numbers(2)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1;

SELECT count() FROM t_04827_dst;

TRUNCATE TABLE t_04827_dst;

-- Single-row blocks are buffered, but appending a block that would push the buffer over the
-- max_rows threshold first flushes the buffered data to the destination: the third block of the
-- INSERT triggers the second in-query flush, whose nested INSERT used to count the part committed
-- by the first one.
CREATE TABLE t_04827_buf_flush (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_04827_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

INSERT INTO t_04827_buf_flush SELECT number FROM numbers(3)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1;

-- Two rows are flushed to the destination and the third one still sits in the buffer.
SELECT count() FROM t_04827_dst;
SELECT count() FROM t_04827_buf_flush;

-- Let the buffered row flush on DROP without tripping the limit.
ALTER TABLE t_04827_dst MODIFY SETTING parts_to_throw_insert = 1000;

DROP TABLE t_04827_buf_flush;
DROP TABLE t_04827_buf_bypass;
DROP TABLE t_04827_dst;
