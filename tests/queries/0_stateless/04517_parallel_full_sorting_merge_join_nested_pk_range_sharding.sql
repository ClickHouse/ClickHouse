-- Regression test for the `disableVirtualRowInSubtree` boundary in `optimizeParallelFullSortingMergeJoin`,
-- for the nested-sort implementation `04516` does not cover: a `PartitionedFinishSorting`.
--
-- When `parallel_full_sorting_merge` scatters an already-sorted side, that side's read-in-order MergeTree
-- sources must stop emitting virtual rows (a scattered virtual row would surface as a spurious result row).
-- The walk that disables them stops at a nested `SortingStep` only when that sort actually consumes or
-- removes those rows (`FinishSorting` / `MergingSorted` / `Full` all run a `MergingSortedTransform` or a
-- `RemoveVirtualRowTransform`). A `Type::PartitionedFinishSorting` from the primary-key-range sharding path
-- (`query_plan_join_shard_by_pk_ranges = 1`, `SortingStep::convertToPartitionedFinishSorting`) is not such a
-- boundary: with `scatter_partitions == 0` it only finishes the sort suffix within each partition and never
-- merges, so it does not consume the source virtual rows below it. The walk must keep descending through it
-- and reset those sources, or they would reach the outer `ScatterByPartitionTransform` unchanged (through
-- `JoiningTransform`, which forwards virtual rows) and surface as spurious result rows.
--
-- `04516` pins `query_plan_join_shard_by_pk_ranges = 0`, so its nested read-in-order join stays a
-- `FinishSorting` (consuming) boundary. Here the nested inner join is a `full_sorting_merge` sharded by
-- primary-key ranges, so its pre-sorts become `PartitionedFinishSorting` (non-consuming) - proving the
-- boundary for that second implementation too. The result must still match `hash` and `full_sorting_merge`
-- on both analyzers, with virtual rows off and on.
--
-- The settings randomized in CI are pinned so the shape is reached deterministically: the outer join stays
-- on this algorithm's hash-scatter path (`join_algorithm = 'parallel_full_sorting_merge'`, `max_threads`),
-- the inner join is sharded at the source (`query_plan_join_shard_by_pk_ranges = 1`,
-- `query_plan_join_swap_table = 0`, small `index_granularity` so several granules split into layers), and
-- the ordered side reads in order (`optimize_read_in_order = 1`).
--
-- Parallel replicas are disabled: under the `ParallelReplicas` CI profile the ordered side is read through
-- the parallel-replicas coordinator instead of a local in-order MergeTree read, so the primary-key-range
-- sharding path does not apply and the inner join is not sharded at source (`both_joins_sharded` would drop
-- to a single `Sharding:` marker). The correctness checks stay valid either way; the plan-shape checks need
-- the local read.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS pfsmj_pkr_ord;
DROP TABLE IF EXISTS pfsmj_pkr_dim;
DROP TABLE IF EXISTS pfsmj_pkr_probe;

-- Small `index_granularity` so the modest row counts still produce enough granules for the primary-key-range
-- path to split both ordered inputs of the inner join into per-shard layers (regardless of CI's randomized
-- default granularity).
CREATE TABLE pfsmj_pkr_ord (k UInt64, d UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
CREATE TABLE pfsmj_pkr_dim (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256;
CREATE TABLE pfsmj_pkr_probe (j UInt64, y UInt64) ENGINE = MergeTree ORDER BY y SETTINGS index_granularity = 256;

-- Overlapping inserts give several parts on each ordered side, so the read-in-order emits per-part virtual
-- rows and the layer split has intersecting ranges to distribute. Duplicate keys give many-to-many matches
-- within a shard. `probe` is ordered by `y`, not by the outer join key `j`, so the outer join cannot be
-- primary-key-range sharded and stays on the `parallel_full_sorting_merge` scatter path.
INSERT INTO pfsmj_pkr_ord SELECT number, number % 100 FROM numbers(0, 12000);
INSERT INTO pfsmj_pkr_ord SELECT number, number % 100 FROM numbers(6000, 12000);
INSERT INTO pfsmj_pkr_dim SELECT number, number * 2 FROM numbers(0, 12000);
INSERT INTO pfsmj_pkr_dim SELECT number, number * 2 FROM numbers(6000, 12000);
INSERT INTO pfsmj_pkr_probe SELECT number, number FROM numbers(9000);

-- Analyzer path (enable_analyzer = 1).

-- The outer join is scattered by the hash of the join key (`ScatterByPartitionTransform`, one per side).
SELECT 'analyzer outer_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s
  INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0);

-- Both joins are sharded: the outer one by the hash scatter, the inner `full_sorting_merge` one by primary-key
-- ranges (its pre-sorts become `PartitionedFinishSorting`). The primary-key-range path adds no
-- `ScatterByPartitionTransform`, so a second `Sharding` marker here is the inner join being sharded at source.
SELECT 'analyzer both_joins_sharded', countIf(explain LIKE '%Sharding:%') = 2
FROM (EXPLAIN actions = 1
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s
  INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0);

-- The inner join really reads its MergeTree sides in order (the nested read-in-order the boundary protects).
SELECT 'analyzer inner_reads_in_order', countIf(explain LIKE '%InOrder%') >= 1
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s
  INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0);

-- Correctness against `hash` and `full_sorting_merge`, with virtual rows off, on, and per-block on.
SELECT 'analyzer virtual_row_off',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 0, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'hash');

SELECT 'analyzer virtual_row_on',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'full_sorting_merge');

SELECT 'analyzer virtual_row_per_block_on',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'hash');

-- A LEFT outer join in the subquery keeps every ordered row (also non-`LEFT ANY/ALL`); still correct.
SELECT 'analyzer inner_left',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l LEFT JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l LEFT JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'hash');

-- Row-level check: the two result sets must be identical, not just their aggregates.
SELECT 'analyzer rows_identical', count() = 0
FROM
(
    (SELECT s.k, p.y FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0)
    EXCEPT
    (SELECT s.k, p.y FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'hash')
);

-- Legacy analyzer: the nested primary-key-range-sharded subquery must scatter and stay correct too.
-- `enable_analyzer` cannot be changed inside a subquery, so set it at session level (as in `04516`).
SET enable_analyzer = 0;

SELECT 'legacy outer_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 2
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s
  INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0);

SELECT 'legacy virtual_row_on',
    (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0)
  = (SELECT (sum(s.k), sum(p.y), count()) FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j SETTINGS join_algorithm = 'hash');

SET enable_analyzer = 1;

DROP TABLE pfsmj_pkr_ord;
DROP TABLE pfsmj_pkr_dim;
DROP TABLE pfsmj_pkr_probe;
