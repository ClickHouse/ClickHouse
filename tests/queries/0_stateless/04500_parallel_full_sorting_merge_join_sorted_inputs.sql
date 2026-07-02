-- `parallel_full_sorting_merge` is advertised as working on any sorted input, not only MergeTree read in
-- order. When the input is already sorted by the join key (a sorted subquery, a sorted `UNION ALL`, or any
-- other sorted upstream operator), `applyOrder` turns the pre-join full sort into a `FinishSorting`. Such a
-- `FinishSorting` must still be scattered into per-shard merge joins (`ScatterByPartitionTransform`): the
-- scatter re-establishes a full sort inside each shard, so it is correct, and the algorithm keeps its
-- advertised scope. Only read-in-order MergeTree reads (which carry buffering / virtual-row chunks) are
-- deliberately left to the primary-key-range sharding path, so they are NOT hash-scattered here.
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

-- Read-in-order MergeTree reads are NOT hash-scattered here: they are handled by the primary-key-range path.
SELECT 'analyzer read_in_order_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1);

-- Same with virtual rows enabled: still not hash-scattered.
SELECT 'analyzer read_in_order_virtual_row_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT l.k FROM pfsmj_rio_left AS l INNER JOIN pfsmj_rio_right AS r ON l.k = r.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1);

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
