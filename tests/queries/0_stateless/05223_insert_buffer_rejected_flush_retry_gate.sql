-- When a `Buffer` flush is rejected by the `Too many parts` check, the rows go back into the buffer
-- and are written again by a later flush. That later flush is a new write: it must look at the table
-- as it is then, not inherit the rejection made for the write that did not happen. Otherwise the rows
-- would stay in the buffer for good once the limit has been raised or the parts have been merged
-- away, and a query whose rows are flushed together with them would be rejected as well.

DROP TABLE IF EXISTS t_05223_dst;
DROP TABLE IF EXISTS t_05223_buf;

CREATE TABLE t_05223_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- A single row fits into the buffer; the next row flushes it to the destination.
CREATE TABLE t_05223_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05223_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

-- One part is already there: the next write into the destination is rejected.
INSERT INTO t_05223_dst VALUES (100);

-- Query A: the second block flushes the first one, and that write is rejected. The first row stays
-- in the buffer, together with the record of query A the rejection was made for.
INSERT INTO t_05223_buf SELECT number FROM numbers(2)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1; -- { serverError TOO_MANY_PARTS }

SELECT count() FROM t_05223_dst;
SELECT count() FROM t_05223_buf;

-- The limit is raised: the destination accepts writes again.
ALTER TABLE t_05223_dst MODIFY SETTING parts_to_throw_insert = 1000;

-- Query B: appending its row flushes the row of query A. That flush used to rethrow the rejection made
-- for query A and fail query B, although nothing is wrong with the destination any more.
INSERT INTO t_05223_buf VALUES (10);

SELECT count() FROM t_05223_dst;
SELECT count() FROM t_05223_buf;

-- The row of query B is flushed on demand.
OPTIMIZE TABLE t_05223_buf;

SELECT count() FROM t_05223_dst;
SELECT n FROM t_05223_dst ORDER BY n;

DROP TABLE t_05223_buf;
DROP TABLE t_05223_dst;

-- The same with the rows of two queries flushed in one block. The rejected write is made on behalf
-- of query A by `OPTIMIZE`; then query B buffers its row next to the rows of query A, and a flush of
-- all of them together must not fail because of the rejection made for query A earlier.

CREATE TABLE t_05223_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- Three rows fit into the buffer.
CREATE TABLE t_05223_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05223_dst, 1, 1000000, 1000000, 1000000, 3, 1000000000, 1000000000);

INSERT INTO t_05223_dst VALUES (100);

-- Query A: both rows are buffered.
INSERT INTO t_05223_buf VALUES (0), (1);

-- The flush on behalf of query A is rejected; its rows stay in the buffer.
OPTIMIZE TABLE t_05223_buf; -- { serverError TOO_MANY_PARTS }

SELECT count() FROM t_05223_dst;
SELECT count() FROM t_05223_buf;

-- Query B: its row fits next to the rows of query A.
INSERT INTO t_05223_buf VALUES (10);

SELECT count() FROM t_05223_buf;

ALTER TABLE t_05223_dst MODIFY SETTING parts_to_throw_insert = 1000;

-- The rows of both queries are flushed in one block. It used to rethrow the rejection made for
-- query A, so neither its rows nor the row of query B could ever leave the buffer.
OPTIMIZE TABLE t_05223_buf;

SELECT count() FROM t_05223_dst;
SELECT n FROM t_05223_dst ORDER BY n;

DROP TABLE t_05223_buf;
DROP TABLE t_05223_dst;
