-- The `Too many parts` gate of an INSERT is spent on the first write of that query's own rows. A
-- threshold flush of a `Buffer` table evicts whatever the buffer holds at that moment, which can be
-- the data of an earlier query: such a flush has to run the check on its own, otherwise it consumes
-- the gate of the running query before that query has written anything, and the query's own first
-- write skips the check and goes past `parts_to_throw_insert`.

DROP TABLE IF EXISTS t_05023_dst;
DROP TABLE IF EXISTS t_05023_buf;

CREATE TABLE t_05023_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

-- A single row fits into the buffer; the next row flushes it to the destination.
CREATE TABLE t_05023_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05023_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

-- Leave one row of an earlier query in the buffer. The destination is still empty.
INSERT INTO t_05023_buf VALUES (0);

SELECT count() FROM t_05023_dst;

-- The first block of this query flushes the row of the previous query - that nested INSERT runs its
-- own check and passes, committing a part - and the second block flushes the first one, which is the
-- first write of this query's own rows and must observe the part that is already there.
INSERT INTO t_05023_buf SELECT number + 1 FROM numbers(2)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1; -- { serverError TOO_MANY_PARTS }

-- Only the row of the earlier query made it to the destination.
SELECT count() FROM t_05023_dst;

-- Let the buffered rows flush on DROP without tripping the limit.
ALTER TABLE t_05023_dst MODIFY SETTING parts_to_throw_insert = 1000;

DROP TABLE t_05023_buf;
DROP TABLE t_05023_dst;
