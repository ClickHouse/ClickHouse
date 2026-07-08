-- `parallel_full_sorting_merge` is advertised as working on any sorted input, not only MergeTree read in
-- order. Every already-sorted side (a `FinishSorting`, whether it comes from a read-in-order MergeTree read
-- or from `applyOrder` on a sorted subquery / sorted `UNION ALL` / any other sorted upstream operator) is
-- scattered into per-shard merge joins by the hash of the join keys (`ScatterByPartitionTransform`). A side
-- produced by `applyOrder` (non-buffering `FinishSorting`) has its sort re-established inside each shard,
-- while a read-in-order side (buffering `FinishSorting`) has its sort finished per shard without redoing it;
-- either way the result is correct and the algorithm keeps its advertised scope.
--
-- The join key is an integer (hash-compatible with the merge-join comparison), so sharding is not disabled
-- for a key-type reason. `max_threads = 4` keeps the shard count > 1 on any runner. Two
-- `ScatterByPartitionTransform` (one per side) appear when the join is sharded.

DROP TABLE IF EXISTS pfsmj_rio_left;
DROP TABLE IF EXISTS pfsmj_rio_right;

CREATE TABLE pfsmj_rio_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE pfsmj_rio_right (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Several parts so read-in-order emits per-part virtual rows.
INSERT INTO pfsmj_rio_left SELECT number, number FROM numbers(0, 2000);
INSERT INTO pfsmj_rio_left SELECT number, number FROM numbers(2000, 2000);
INSERT INTO pfsmj_rio_right SELECT number, number * 2 FROM numbers(0, 4000);

-- Analyzer path (enable_analyzer = 1).

-- Generic already-sorted subqueries (the `ORDER BY k` is preserved because the query returns `l.k`, so the
-- pre-join sort is a `FinishSorting`): must be scattered.
SELECT 'analyzer sorted_subquery_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
  INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- A sorted `UNION ALL` on one side is also a generic sorted input: must be scattered.
SELECT 'analyzer sorted_union_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number AS k FROM numbers(2000) UNION ALL SELECT number AS k FROM numbers(2000) ORDER BY k) AS l
  INNER JOIN (SELECT number AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

-- Read-in-order MergeTree reads are also scattered (a read-in-order side is a buffering `FinishSorting`,
-- scattered while preserving order so each shard finishes the sort instead of redoing it).
-- `query_plan_join_shard_by_pk_ranges = 0` keeps the read-in-order side on this algorithm's hash-scatter
-- path: the PK-range sharding path parallelizes it differently (a `PartitionedFinishSorting`, without
-- `ScatterByPartitionTransform`), and it is randomized in CI, so pin it here.
SELECT 'analyzer read_in_order_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0);

-- Same with virtual rows enabled: still scattered (virtual-row emission is disabled on a scattered side).
SELECT 'analyzer read_in_order_virtual_row_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0);

-- Legacy analyzer: the sorted subquery must scatter too. `enable_analyzer` cannot be changed inside a
-- subquery, so set it at session level (as in `04494` / `04497`).
SET enable_analyzer = 0;

SELECT 'legacy sorted_subquery_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS l
  INNER JOIN (SELECT number % 1000 AS k FROM numbers(4000) ORDER BY k) AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4);

SET enable_analyzer = 1;

-- The scattered sorted-subquery join must return the same (correct) result as the single merge join and hash.
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
