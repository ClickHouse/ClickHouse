-- `parallel_sorted_merge` is `sorted_merge` (a merge join available only when both inputs can be
-- efficiently read in the order of the join keys) additionally sharded by ranges of the tables' common
-- primary-key prefix into independent per-shard merge joins - the same source-side sharding
-- `query_plan_join_shard_by_pk_ranges` applies, but enabled for this join by the algorithm itself, with
-- the setting left at its default 0. The in-order reads stay intact (no `ScatterByPartitionTransform`, no
-- full sort). When the sharding cannot apply (an `ASOF` join), the join degrades to a single `sorted_merge`.

DROP TABLE IF EXISTS psmj_left;
DROP TABLE IF EXISTS psmj_right;

-- Small `index_granularity` so the modest row counts still produce enough granules for the primary-key-range
-- path to split both inputs into per-shard layers (regardless of CI's randomized default granularity).
CREATE TABLE psmj_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;
CREATE TABLE psmj_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

-- Overlapping inserts give several parts per side, so the layer split has intersecting ranges to
-- distribute; duplicate ids exercise many-to-many matches within a shard. The row counts are kept as
-- low as the layer split allows (42 and 50 granules): the correctness cases below run a full join per
-- kind twice, so the data volume is what this test costs, and the flaky-check job runs it 50 times.
INSERT INTO psmj_left SELECT number % 3750, number FROM numbers(0, 5000);
INSERT INTO psmj_left SELECT number % 3750, number FROM numbers(5000, 5000);
INSERT INTO psmj_right SELECT number % 2500, number * 2 FROM numbers(0, 6250);
INSERT INTO psmj_right SELECT number % 2500, number * 3 FROM numbers(6250, 6250);

-- The eligibility of `parallel_sorted_merge` is decided on the query plan, which exists only for the
-- analyzer. The default is overridden to 0 in the old-analyzer CI configuration, so pin it explicitly.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on. `query_plan_join_shard_by_pk_ranges`
-- is pinned to its default 0: the whole point is that `parallel_sorted_merge` shards without it.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

-- Selected on the primary-key join and sharded by primary-key ranges: the `Sharding:` marker appears on
-- the join even though `query_plan_join_shard_by_pk_ranges = 0`.
SELECT 'parallel_sorted_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT l.a FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- The sharding is at the source: in-order reads, no shuffle and no re-sort in the pipeline.
SELECT 'no_scatter_no_resort', countIf(explain LIKE '%ScatterByPartitionTransform%') = 0 AND countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- ...running a merge join.
SELECT 'runs_merge_join', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.a FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- Plain `sorted_merge` is NOT sharded with the setting at 0: the sharding belongs to the parallel variant.
SELECT 'sorted_merge_not_sharded', countIf(explain LIKE '%Sharding:%') = 0
FROM (EXPLAIN actions = 1 SELECT l.a FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- Neither is `full_sorting_merge` (the pre-existing behavior must not change).
SELECT 'full_sorting_merge_not_sharded', countIf(explain LIKE '%Sharding:%') = 0
FROM (EXPLAIN actions = 1 SELECT l.a FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- A join key that is not a primary-key prefix makes `parallel_sorted_merge` ineligible: falls through to
-- `hash`, no merge join.
SELECT 'unsorted_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.a = r.b SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- Priority order is respected: `hash` first wins even though `parallel_sorted_merge` is eligible.
SELECT 'priority_order_respected', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash,parallel_sorted_merge', max_threads = 4);

-- Correctness against `hash` for every join kind.
SELECT 'inner',
    (SELECT (sum(l.a + r.b), count()) FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a + r.b), count()) FROM psmj_left AS l INNER JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'left',
    (SELECT (sum(l.a), count()) FROM psmj_left AS l LEFT JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a), count()) FROM psmj_left AS l LEFT JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'right',
    (SELECT (sum(r.b), count()) FROM psmj_left AS l RIGHT JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(r.b), count()) FROM psmj_left AS l RIGHT JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full',
    (SELECT (sum(l.a), sum(r.b), count()) FROM psmj_left AS l FULL JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM psmj_left AS l FULL JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full_use_nulls',
    (SELECT (sum(l.a), sum(r.b), count()) FROM psmj_left AS l FULL JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', join_use_nulls = 1)
  = (SELECT (sum(l.a), sum(r.b), count()) FROM psmj_left AS l FULL JOIN psmj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash', join_use_nulls = 1);

-- An `ASOF` join cannot be sharded by primary-key ranges (its trailing key is the inequality key), so it
-- degrades to a single in-order merge join: still selected (merge join in the pipeline), no `Sharding:`
-- marker, and the result matches `hash`.
SELECT 'asof_not_sharded', countIf(explain LIKE '%Sharding:%') = 0
FROM (EXPLAIN actions = 1 SELECT l.id FROM psmj_left AS l ASOF INNER JOIN psmj_right AS r ON l.id = r.id AND l.a > r.b SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

SELECT 'asof_merge_join', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.id FROM psmj_left AS l ASOF INNER JOIN psmj_right AS r ON l.id = r.id AND l.a > r.b SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

SELECT 'asof_result',
    (SELECT (sum(l.a + r.b), count()) FROM psmj_left AS l ASOF INNER JOIN psmj_right AS r ON l.id = r.id AND l.a > r.b SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a + r.b), count()) FROM psmj_left AS l ASOF INNER JOIN psmj_right AS r ON l.id = r.id AND l.a > r.b SETTINGS join_algorithm = 'hash');

-- A pure `ASOF` join has no equality keys, so the inequality key itself must be probed for in-order
-- eligibility. Both sorted variants select the merge join; the parallel variant still degrades to a
-- single stream because ASOF joins cannot be sharded by primary-key ranges.
SELECT 'pure_asof_sorted_merge', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.id FROM psmj_left AS l ASOF LEFT JOIN psmj_right AS r ON l.id >= r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

SELECT 'pure_asof_parallel_merge', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.id FROM psmj_left AS l ASOF LEFT JOIN psmj_right AS r ON l.id >= r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

SELECT 'pure_asof_parallel_not_sharded', countIf(explain LIKE '%Sharding:%') = 0
FROM (EXPLAIN actions = 1 SELECT l.id FROM psmj_left AS l ASOF LEFT JOIN psmj_right AS r ON l.id >= r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

DROP TABLE psmj_left;
DROP TABLE psmj_right;
