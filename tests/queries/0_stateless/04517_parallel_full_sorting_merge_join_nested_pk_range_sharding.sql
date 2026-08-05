-- A sorted subquery whose `ORDER BY` comes out of a nested `full_sorting_merge` join sharded by primary-key
-- ranges (`query_plan_join_shard_by_pk_ranges = 1`, whose pre-sorts become `PartitionedFinishSorting`),
-- used as one side of an outer `parallel_full_sorting_merge` join - the nested-sort implementation `04516`
-- does not cover.
--
-- Such an already-sorted (`FinishSorting`) outer side must NOT be scattered (an order-preserving scatter
-- into the per-shard merges can deadlock the pipeline), so the outer join falls back to a single merge
-- join. The nested reads' virtual rows stay intact: a `PartitionedFinishSorting` does not consume them (it
-- only finishes the sort suffix within each pre-partitioned stream, and `JoiningTransform` forwards them),
-- so they reach the outer pre-join sort's merge exactly as they do under plain `full_sorting_merge`. The
-- result must still match `hash` and `full_sorting_merge` on both analyzers, with virtual rows off and on.
--
-- The settings randomized in CI are pinned so the shape is reached deterministically: the inner join is
-- sharded at the source (`query_plan_join_shard_by_pk_ranges = 1`, `query_plan_join_swap_table = 0`, small
-- `index_granularity` so several granules split into layers), and the ordered side reads in order
-- (`optimize_read_in_order = 1`).
--
-- Parallel replicas are disabled: under the `ParallelReplicas` CI profile the ordered side is read through
-- the parallel-replicas coordinator instead of a local in-order MergeTree read, so the primary-key-range
-- sharding path does not apply and the inner join is not sharded at source (`both_joins_sharded` would drop
-- to a single `Sharding:` marker). The correctness checks stay valid either way; the plan-shape checks need
-- the local read.

SET enable_parallel_replicas = 0;

-- `optimize_sorting_by_input_stream_properties` (randomized in CI) is what turns the outer pre-join sort
-- atop the sorted subquery into a `FinishSorting`; with it off the outer sides stay plain full sorts and
-- ARE scattered (the safe, fully-draining path), flipping `outer_not_scattered` and `inner_join_sharded` -
-- so pin it.
SET optimize_sorting_by_input_stream_properties = 1;

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
-- primary-key-range sharded (and, its left side being pre-sorted, it is not hash-scattered either - it runs
-- as a single merge join).
INSERT INTO pfsmj_pkr_ord SELECT number, number % 100 FROM numbers(0, 12000);
INSERT INTO pfsmj_pkr_ord SELECT number, number % 100 FROM numbers(6000, 12000);
INSERT INTO pfsmj_pkr_dim SELECT number, number * 2 FROM numbers(0, 12000);
INSERT INTO pfsmj_pkr_dim SELECT number, number * 2 FROM numbers(6000, 12000);
INSERT INTO pfsmj_pkr_probe SELECT number, number FROM numbers(9000);

-- Analyzer path. The default is overridden to 0 in the old-analyzer CI configuration, so pin it
-- explicitly (`enable_analyzer` cannot be changed inside a subquery, so set it at session level).
SET enable_analyzer = 1;

-- The outer join is NOT scattered: its left side is a sorted subquery (a pre-sorted `FinishSorting` side),
-- so the sharded rewrite must not fire.
SELECT 'analyzer outer_not_scattered', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0
FROM (EXPLAIN PIPELINE
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s
  INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0);

-- Only the inner `full_sorting_merge` join is sharded - by primary-key ranges at the source (its pre-sorts
-- become `PartitionedFinishSorting`); the outer join stays a single merge join, so exactly one `Sharding`
-- marker appears.
SELECT 'analyzer inner_join_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1
  SELECT s.k FROM (SELECT l.k AS k FROM pfsmj_pkr_ord AS l INNER JOIN pfsmj_pkr_dim AS r ON l.k = r.k ORDER BY l.k SETTINGS join_algorithm = 'full_sorting_merge') AS s
  INNER JOIN pfsmj_pkr_probe AS p ON s.k = p.j
  SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, optimize_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0);

-- The inner join really reads its MergeTree sides in order (the nested read-in-order this test is about).
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

-- Legacy analyzer: `InterpreterSelectQuery` does not recognize the subquery's sortedness (no `applyOrder`),
-- so the outer pre-join sorts stay plain full sorts - which ARE scattered (the safe, fully-draining path)
-- - and the result must stay correct. `enable_analyzer` cannot be changed inside a subquery, so set it at
-- session level (as in `04516`).
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
