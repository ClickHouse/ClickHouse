-- Tags: no-parallel-replicas, no-old-analyzer
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- `EXPLAIN ANALYZE` reports the block nested loop join: the two per-side lines every join step prints,
-- and the two describing the build side it materializes. Which sides can report `matched` is a matter
-- of the kind, and is documented rather than pinned here; this only asserts that the numbers arrive and
-- that they are the numbers of the join.

SET enable_analyzer = 1;
SET allow_block_nested_loop_join = 1;
-- An `ALL INNER JOIN` whose `ON` section determines no join key becomes a cartesian product with a
-- filter where the hash join is available, and reaches the operator only where it is not.
SET join_algorithm = 'full_sorting_merge';
SET query_plan_join_swap_table = 'false';
-- The `Buffer:` and `Spill:` lines report how the build side was held, so what decides that has to be
-- pinned rather than left to the thresholds the test harness randomizes.
SET cross_join_min_rows_to_compress = 0, cross_join_min_bytes_to_compress = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS bnl_l;
DROP TABLE IF EXISTS bnl_r;
CREATE TABLE bnl_l (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bnl_r (id Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO bnl_l SELECT number + 1, number % 5 FROM numbers(10);
INSERT INTO bnl_r SELECT number + 1, number % 4 FROM numbers(8);

-- Of the 10 probe rows the 6 with `x < 3` find a partner, and of the 8 build rows the 6 with `y > 0` do.
SELECT countIf(explain LIKE '%Left: rows 10.00 · matched 6.00 · match rate 60.00%') = 1
    AND countIf(explain LIKE '%Right: rows 8.00 · matched 6.00 · match rate 75.00%') = 1
    AND countIf(explain LIKE '%Buffer: memory %· compressed no%') = 1
    AND countIf(explain LIKE '%Spill: no%') = 1
FROM (EXPLAIN ANALYZE SELECT l.id, r.id FROM bnl_l l ALL FULL JOIN bnl_r r ON l.x < r.y);

-- `matches = 1` buys the probe side's count where the join does not need it for the result itself.
SELECT countIf(explain LIKE '%Left: rows 10.00 · matched 6.00%') = 1
FROM (EXPLAIN ANALYZE matches = 1 SELECT l.id, r.id FROM bnl_l l ALL INNER JOIN bnl_r r ON l.x < r.y);

DROP TABLE bnl_l;
DROP TABLE bnl_r;
