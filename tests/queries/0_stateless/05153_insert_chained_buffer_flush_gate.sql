-- A `Buffer` flush writes on behalf of every query whose rows it evicts, and the pre-write decision
-- it makes has to be observed by all of them. When the destination of a `Buffer` is another `Buffer`,
-- the flushed block is buffered again together with the group of queries it was written for, and the
-- second buffer lists that group as a single participant of its own flush. The queries behind it are
-- one level deeper, but they are the ones that own the rows: a query whose rows reached the final
-- destination through such a chain must not run the `Too many parts` check again afterwards and count
-- the part its own rows created.

DROP TABLE IF EXISTS t_05153_dst;
DROP TABLE IF EXISTS t_05153_buf_inner;
DROP TABLE IF EXISTS t_05153_buf_outer;

CREATE TABLE t_05153_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- Both buffers hold a single row: every block after the first one flushes the row that is there.
CREATE TABLE t_05153_buf_inner (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05153_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

CREATE TABLE t_05153_buf_outer (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05153_buf_inner, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

-- Four single-row blocks: the outer buffer flushes three of them into the inner one, which in turn
-- flushes two of them into the destination. The second of those two writes used to count the part
-- committed by the first one - both of them made out of this query's own rows - and fail.
INSERT INTO t_05153_buf_outer SELECT number FROM numbers(4)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1;

-- Two rows reached the destination, one is left in each buffer. A SELECT from a `Buffer` reads its
-- own rows and the rows of its destination, so the counts add up along the chain.
SELECT count() FROM t_05153_dst;
SELECT count() FROM t_05153_buf_inner;
SELECT count() FROM t_05153_buf_outer;

-- Let the buffered rows flush on DROP without tripping the limit.
ALTER TABLE t_05153_dst MODIFY SETTING parts_to_throw_insert = 1000;

DROP TABLE t_05153_buf_outer;
DROP TABLE t_05153_buf_inner;
DROP TABLE t_05153_dst;
