-- Tags: no-replicated-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/70212
-- Trivial limit optimization must not reduce the number of rows scanned
-- when parts contain lightweight deletes, because masked rows reduce
-- the effective row count below the physical row count.

DROP TABLE IF EXISTS t_trivial_limit_lwd;

CREATE TABLE t_trivial_limit_lwd (id UInt64, v String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128;

-- Insert enough rows so that the trivial limit optimization would kick in
-- (limit + offset < max_block_size).
INSERT INTO t_trivial_limit_lwd SELECT number, toString(number) FROM numbers(1000);

-- Delete roughly half the rows via lightweight delete.
DELETE FROM t_trivial_limit_lwd WHERE id % 2 = 0;

-- Without the fix, the trivial limit optimization reduces block_size to 10
-- and num_streams to 1, reading only 10 physical rows. After filtering out
-- deleted rows, fewer than 10 live rows remain — the query returns too few.
SELECT count() FROM (SELECT * FROM t_trivial_limit_lwd LIMIT 10);

-- Verify exact row content is correct (all returned rows must be odd).
SELECT id FROM t_trivial_limit_lwd ORDER BY id LIMIT 10;

-- Also test with OFFSET.
SELECT count() FROM (SELECT * FROM t_trivial_limit_lwd LIMIT 5 OFFSET 5);

-- After OPTIMIZE (which physically removes deleted rows), trivial limit
-- should work normally again.
OPTIMIZE TABLE t_trivial_limit_lwd FINAL SETTINGS mutations_sync = 2;
SELECT count() FROM (SELECT * FROM t_trivial_limit_lwd LIMIT 10);

DROP TABLE t_trivial_limit_lwd;
