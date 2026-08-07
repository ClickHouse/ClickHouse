-- An already-sorted side (a `FinishSorting`, whether it comes from a read-in-order MergeTree read or from
-- `applyOrder` on a sorted subquery / sorted `UNION ALL` / any other sorted upstream operator) must NOT be
-- scattered by `parallel_full_sorting_merge`: an order-preserving scatter into the per-shard merges can
-- deadlock the pipeline (single-chunk ports; the scatter does not consume new input until all partition
-- chunks of the previous one are pushed, while the per-partition merges and per-shard merge joins consume
-- selectively - two such scatters form a circular wait). The join falls back to a single merge join,
-- exactly like `full_sorting_merge`, and the result must still be correct.
--
-- The join key is an integer (hash-compatible with the merge-join comparison), so sharding is not skipped
-- for a key-type reason: the pre-sorted side is the only thing preventing it. `max_threads = 4` keeps the
-- shard count > 1 on any runner.
--
-- `optimize_sorting_by_input_stream_properties` (randomized in CI) is what turns the pre-join sort atop a
-- generic sorted subquery / sorted `UNION ALL` into a `FinishSorting`; with it off those sides stay plain
-- full sorts and ARE scattered (the safe, fully-draining path), flipping the plan-shape checks - so pin it.
SET optimize_sorting_by_input_stream_properties = 1;

DROP TABLE IF EXISTS pfsmj_rio_left;
DROP TABLE IF EXISTS pfsmj_rio_right;

CREATE TABLE pfsmj_rio_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE pfsmj_rio_right (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Several parts so read-in-order emits per-part virtual rows.
INSERT INTO pfsmj_rio_left SELECT number, number FROM numbers(0, 2000);
INSERT INTO pfsmj_rio_left SELECT number, number FROM numbers(2000, 2000);
INSERT INTO pfsmj_rio_right SELECT number, number * 2 FROM numbers(0, 4000);

-- Analyzer path. The default is overridden to 0 in the old-analyzer CI configuration, so pin it
-- explicitly (`enable_analyzer` cannot be changed inside a subquery, so set it at session level).
SET enable_analyzer = 1;

-- Generic already-sorted subqueries (the `ORDER BY k` is preserved because the query returns `l.k`, so the
-- pre-join sort is a `FinishSorting`): must NOT be scattered.
SELECT 'analyzer sorted_subquery_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
  INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- A sorted `UNION ALL` on one side is also a generic sorted input: must NOT be scattered.
SELECT 'analyzer sorted_union_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number AS k FROM numbers(2000) UNION ALL SELECT number AS k FROM numbers(2000) ORDER BY k) AS l
  INNER JOIN (SELECT number AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- Read-in-order MergeTree reads are pre-sorted sides too: must NOT be scattered.
-- `query_plan_join_shard_by_pk_ranges = 0` keeps the PK-range sharding path (which parallelizes a
-- read-in-order join at the source, without a scatter) out of the picture; it is randomized in CI, so pin
-- it here.
SELECT 'analyzer read_in_order_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0);

-- Same with virtual rows enabled: still not scattered (and the virtual rows stay intact on the single
-- merge join).
SELECT 'analyzer read_in_order_virtual_row_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0);

-- Contrast: unsorted subqueries (no `ORDER BY`) build plain full sorts, which ARE scattered - proving the
-- checks above verify the pre-sorted-side fallback and not an unrelated failure to shard.
SELECT 'analyzer unsorted_subquery_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number % 1000 AS k FROM numbers(4000)) AS l
  INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000)) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- Legacy analyzer: `InterpreterSelectQuery` does not recognize the subquery's sortedness (no `applyOrder`),
-- so the pre-join sorts stay plain full sorts - which ARE scattered (the safe, fully-draining path).
-- `enable_analyzer` cannot be changed inside a subquery, so set it at session level (as in `04494` /
-- `04497`).
SET enable_analyzer = 0;

SELECT 'legacy sorted_subquery_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
  INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

SET enable_analyzer = 1;

-- The sorted-subquery join (fallback single merge join) must return the same (correct) result as
-- `full_sorting_merge` and `hash`.
SELECT 'result_all_equal',
    (SELECT (sum(l.k), count()) FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
       INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
       SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.k), count()) FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
       INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
       SETTINGS join_algorithm = 'full_sorting_merge')
  AND
    (SELECT (sum(l.k), count()) FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
       INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
       SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4)
  = (SELECT (sum(l.k), count()) FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
       INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
       SETTINGS join_algorithm = 'hash');

DROP TABLE pfsmj_rio_left;
DROP TABLE pfsmj_rio_right;
