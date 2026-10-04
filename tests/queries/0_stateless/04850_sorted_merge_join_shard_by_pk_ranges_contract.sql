-- The algorithm name encodes the parallelism choice: `sorted_merge` is the single-stream variant and
-- `parallel_sorted_merge` is the one sharded by primary-key ranges. The blanket
-- `query_plan_join_shard_by_pk_ranges = 1` opt-in must not override the explicit single-stream choice
-- and turn `sorted_merge` into `parallel_sorted_merge`, while the pre-existing algorithms
-- (`full_sorting_merge`, `hash`) keep being sharded by it as before.

DROP TABLE IF EXISTS smj_left;
DROP TABLE IF EXISTS smj_right;

-- Small `index_granularity` so the modest row counts still produce enough granules for the primary-key-range
-- path to split both inputs into per-shard layers (regardless of CI's randomized default granularity).
CREATE TABLE smj_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;
CREATE TABLE smj_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

INSERT INTO smj_left SELECT number % 30000, number FROM numbers(0, 40000);
INSERT INTO smj_left SELECT number % 30000, number FROM numbers(40000, 40000);
INSERT INTO smj_right SELECT number % 20000, number * 2 FROM numbers(0, 50000);
INSERT INTO smj_right SELECT number % 20000, number * 3 FROM numbers(50000, 50000);

-- The eligibility of the sorted-merge algorithms is decided on the query plan, which exists only for the
-- analyzer. The default is overridden to 0 in the old-analyzer CI configuration, so pin it explicitly.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on, with the global sharding opt-in ON.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 1, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

-- `sorted_merge` stays single-stream even under `query_plan_join_shard_by_pk_ranges = 1`: no `Sharding:`
-- marker on the join.
SELECT 'sorted_merge_stays_single_stream', countIf(explain LIKE '%Sharding:%') = 0
FROM (EXPLAIN actions = 1 SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- ...and still runs a single merge join.
SELECT 'sorted_merge_runs_merge_join', countIf(explain LIKE '%MergeJoinTransform%') = 1
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- `parallel_sorted_merge` is sharded, with or without the setting (it enables the sharding itself).
SELECT 'parallel_sorted_merge_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- The pre-existing behavior of the setting must not change: `full_sorting_merge` is sharded under
-- `query_plan_join_shard_by_pk_ranges = 1`.
SELECT 'full_sorting_merge_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'full_sorting_merge', max_threads = 4);

-- The results of the single-stream and the sharded variants agree under the setting.
SELECT 'results_agree',
    (SELECT sum(cityHash64(l.id, l.a, r.b)) FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4)
  = (SELECT sum(cityHash64(l.id, l.a, r.b)) FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

DROP TABLE smj_left;
DROP TABLE smj_right;
