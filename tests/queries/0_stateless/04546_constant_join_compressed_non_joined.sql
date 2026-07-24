-- `ConstantJoin` compresses the stored right-side blocks when the `cross_join_min_{rows,bytes}_to_compress`
-- thresholds are reached. The non-joined output (`RIGHT` join with a constant-false predicate) then emits
-- a stored block wider than `max_block_size` in several chunks and must decompress it only once.

SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 0;
SET cross_join_min_rows_to_compress = 1;
SET cross_join_min_bytes_to_compress = 1;
SET min_joined_block_size_rows = 0;
SET min_joined_block_size_bytes = 0;
SET max_block_size = 999;

DROP TABLE IF EXISTS t_right_compressed;
-- The Memory engine emits blocks as they were inserted, so the stored blocks are much wider than `max_block_size`.
CREATE TABLE t_right_compressed (n UInt64, s String) ENGINE = Memory;
INSERT INTO t_right_compressed SELECT number, repeat('y', 50) FROM numbers(10000);
INSERT INTO t_right_compressed SELECT number + 10000, repeat('z', 50) FROM numbers(10000);

SELECT count(), uniqExact(n), sum(n), min(length(s)), max(l.x)
FROM (SELECT 1 AS x) AS l
RIGHT JOIN t_right_compressed AS r ON 0;

DROP TABLE t_right_compressed;
