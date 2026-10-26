-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- Tests the estimation-vs-actual join metrics of `EXPLAIN ANALYZE`: `Cost`, `Selectivity`,
-- `Output rows` with `q-error`, per-side estimated rows, input columns, and the accumulation
-- of the actual branch cost over a join-reorder cluster. Timings are non-deterministic, so
-- the test asserts structural invariants and exact quantity strings for pinned row counts.
-- The data is three 100-row tables in a 1:1 relationship on `id`, so every join matches
-- exactly 100 rows and all asserted quantities are deterministic.

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10; -- needed for consistent estimated row counts in EXPLAIN output

DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table3;
CREATE TABLE table1 (id UInt64, v1 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE table2 (id UInt64, v2 String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE table3 (id UInt64, v3 String) ENGINE = MergeTree ORDER BY id;
INSERT INTO table1 SELECT number, toString(number) FROM numbers(100);
INSERT INTO table2 SELECT number, toString(number) FROM numbers(100);
INSERT INTO table3 SELECT number, toString(number) FROM numbers(100);

-- Two-table join: every estimation group is present and the actual values are exact for the
-- pinned data: matched output rows = 100 -> actual cost 100.00, cartesian selectivity
-- 100 / (100 * 100) = 0.01, and a perfect estimate gives q-error 1.00.
SELECT
    countIf(explain LIKE '%Cost: estimated 100.00 · actual 100.00%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01 · actual (cartesian) 0.01%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1,
    countIf(explain LIKE '%Left: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Input (left): id, v1%') = 1,
    countIf(explain LIKE '%Input (right): id, v2%') = 1,
    countIf(explain LIKE '%no stats%') = 0
FROM (EXPLAIN ANALYZE SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id);

-- The same join in a pipeline sharded by primary-key ranges: the join runs as per-shard clones,
-- and the reported actuals must be the sums over the clones, identical to the unsharded run.
SELECT
    countIf(explain LIKE '%Cost: estimated 100.00 · actual 100.00%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01 · actual (cartesian) 0.01%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1,
    countIf(explain LIKE '%Left: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated 100.00 · rows 100.00%') = 1,
    countIf(explain LIKE '%Input (left): id, v1%') = 1,
    countIf(explain LIKE '%Input (right): id, v2%') = 1,
    countIf(explain LIKE '%no stats%') = 0
FROM (EXPLAIN ANALYZE SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id
      SETTINGS query_plan_join_shard_by_pk_ranges = 1);

-- Three-table join in one reorder cluster: the top join's actual cost accumulates the matched
-- output rows of both joins in the cluster (100 + 100 = 200), while the bottom join reports
-- only its own (100).
SELECT
    countIf(explain LIKE '%Cost: estimated 200.00 · actual 200.00%') = 1,
    countIf(explain LIKE '%Cost: estimated 100.00 · actual 100.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 2,
    countIf(explain LIKE '%Left: rows estimated %') = 2,
    countIf(explain LIKE '%Right: rows estimated %') = 2
FROM (EXPLAIN ANALYZE
    SELECT * FROM table1
    INNER JOIN table2 ON table1.id = table2.id
    INNER JOIN table3 ON table2.id = table3.id);

-- Without the join-order optimizer there are no estimates: every estimated field reads
-- `no stats` and `q-error` disappears, but the actual values are still collected.
SELECT
    countIf(explain LIKE '%Cost: estimated no stats · actual 100.00%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) no stats · actual (cartesian) 0.01%') = 1,
    countIf(explain LIKE '%Output rows: estimated no stats · actual 100.00%') = 1,
    countIf(explain LIKE '%q-error%') = 0,
    countIf(explain LIKE '%Left: rows estimated no stats · rows 100.00%') = 1,
    countIf(explain LIKE '%Right: rows estimated no stats · rows 100.00%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id
      SETTINGS query_plan_optimize_join_order_limit = 0);

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;
