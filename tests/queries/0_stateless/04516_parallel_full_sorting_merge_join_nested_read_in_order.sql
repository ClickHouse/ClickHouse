-- Regression test for the `disableVirtualRowInSubtree` boundary in `optimizeParallelFullSortingMergeJoin`.
--
-- When `parallel_full_sorting_merge` scatters an already-sorted side, that side's read-in-order MergeTree
-- sources must stop emitting virtual rows (a scattered virtual row would surface as a spurious result row).
-- But the walk that disables them must stop at any nested `SortingStep` below the scattered sort: an inner
-- sort's `MergingSortedTransform` consumes the virtual rows of the reads beneath it, so those reads never
-- reach the outer scatter. `optimizeReadInOrder` may have admitted such an inner read-in-order plan only
-- because virtual rows were available (for a non-`LEFT ANY/ALL` inner join it otherwise bails out of the
-- in-order read to avoid reading excessive data), so clearing them afterwards would leave the inner plan in a
-- shape the inner optimizer would never have produced.
--
-- This covers the case `04500` does not (its sorted subqueries are `numbers()`-only, with no inner
-- read-in-order MergeTree step): a sorted subquery whose own `ORDER BY` is implemented via read-in-order
-- through an inner join (a nested `SortingStep` with a read-in-order MergeTree read below it), as one side of
-- an outer `parallel_full_sorting_merge` join. The result must match `hash` and `full_sorting_merge`, with
-- virtual rows both off and on, on both analyzers.
--
-- `optimize_read_in_order = 1` and `query_plan_join_shard_by_pk_ranges = 0` (both randomized in CI) are
-- pinned so the read-in-order side stays on this algorithm's hash-scatter path; the inner join is pinned to
-- `hash` so it preserves the subquery's read-in-order instead of becoming a second sharded merge join.

DROP TABLE IF EXISTS pfsmj_nrio_ord;
DROP TABLE IF EXISTS pfsmj_nrio_dim;
DROP TABLE IF EXISTS pfsmj_nrio_probe;

CREATE TABLE pfsmj_nrio_ord (k UInt64, d UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE pfsmj_nrio_dim (d UInt64, x UInt64) ENGINE = MergeTree ORDER BY d;
CREATE TABLE pfsmj_nrio_probe (k UInt64, y UInt64) ENGINE = MergeTree ORDER BY k;

-- Several parts on the ordered side so the read-in-order emits per-part virtual rows. Duplicate keys give
-- many-to-many matches within a shard; a limited `d` range makes the inner join filter rows (so the
-- read-in-order path is the non-`LEFT ANY/ALL`, virtual-row-dependent one the boundary protects).
INSERT INTO pfsmj_nrio_ord SELECT number % 3000, number % 100 FROM numbers(0, 4000);
INSERT INTO pfsmj_nrio_ord SELECT number % 3000, number % 100 FROM numbers(4000, 4000);
INSERT INTO pfsmj_nrio_dim SELECT number, number * 2 FROM numbers(100);
INSERT INTO pfsmj_nrio_probe SELECT number % 2000, number FROM numbers(5000);

-- Analyzer path (enable_analyzer = 1).

-- The outer join is scattered by the hash of the join key (`ScatterByPartitionTransform`, one per side),
-- even though its left side is a sorted subquery that itself reads a MergeTree in order through an inner join.
SELECT 'analyzer scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s
  INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0);

-- The inner join really reads its MergeTree side in order (the nested read-in-order the boundary protects).
SELECT 'analyzer inner_reads_in_order', countIf(explain LIKE '%InOrder%') >= 1
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s
  INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0);

-- Correctness against `hash` and `full_sorting_merge`, with virtual rows off, on, and per-block on.
SELECT 'analyzer virtual_row_off',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 0, query_plan_join_shard_by_pk_ranges = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'hash');

SELECT 'analyzer virtual_row_on',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'full_sorting_merge');

SELECT 'analyzer virtual_row_per_block_on',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1, query_plan_join_shard_by_pk_ranges = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'hash');

-- A LEFT outer join in the subquery keeps every ordered row (also non-`LEFT ANY/ALL`); still correct.
SELECT 'analyzer inner_left',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l LEFT JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l LEFT JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'hash');

-- Row-level check: the two result sets must be identical, not just their aggregates.
SELECT 'analyzer rows_identical', count() = 0
FROM
(
    (SELECT s.k, p.y FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0)
    EXCEPT
    (SELECT s.k, p.y FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'hash')
);

-- Legacy analyzer: the nested read-in-order subquery must scatter and stay correct too. `enable_analyzer`
-- cannot be changed inside a subquery, so set it at session level (as in `04494` / `04497` / `04500`).
SET enable_analyzer = 0;

SELECT 'legacy scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s
  INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0);

SELECT 'legacy virtual_row_on',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_nrio_ord AS l INNER JOIN pfsmj_nrio_dim AS r ON l.d = r.d ORDER BY l.k SETTINGS join_algorithm = 'hash') AS s INNER JOIN pfsmj_nrio_probe AS p ON s.k = p.k SETTINGS join_algorithm = 'hash');

SET enable_analyzer = 1;

DROP TABLE pfsmj_nrio_ord;
DROP TABLE pfsmj_nrio_dim;
DROP TABLE pfsmj_nrio_probe;
