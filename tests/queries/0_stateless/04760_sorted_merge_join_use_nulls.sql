-- `sorted_merge` and `parallel_sorted_merge` remain fully functional under `join_use_nulls = 1`:
-- * `USING` + `join_use_nulls` does not make the merge algorithm unselectable on the analyzer path (the
--   only one where these algorithms exist): the `USING` clause is desugared into an `ON` equality with
--   explicit key casts before physicalization, so the `hasUsing` rejection in
--   `FullSortingMergeJoin::isSupported` never fires there. Listing `sorted_merge` before `hash` really
--   selects the merge join - the eligibility gate suppressing the runtime filter cannot degrade a `hash`
--   fallback, because there is no fallback;
-- * `parallel_sorted_merge` keeps its defining primary-key-range sharding for the OUTER kinds (and for
--   `USING`), where `join_use_nulls` makes the visible output columns - including the keys - `Nullable`.

DROP TABLE IF EXISTS smj_nulls_left;
DROP TABLE IF EXISTS smj_nulls_right;

-- Small `index_granularity` so the modest row counts still produce enough granules for the
-- primary-key-range path to split both inputs into per-shard layers (regardless of CI's randomized
-- default granularity).
CREATE TABLE smj_nulls_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;
CREATE TABLE smj_nulls_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

INSERT INTO smj_nulls_left SELECT number % 30000, number FROM numbers(0, 40000);
INSERT INTO smj_nulls_right SELECT number % 20000, number * 2 FROM numbers(0, 50000);

SET enable_analyzer = 1;
-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
-- Disable automatic spilling, otherwise the printed algorithm name depends on the randomized limits.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SET join_use_nulls = 1;

-- `USING` + `join_use_nulls`: the merge join is selected (no fall-through to `hash`, so suppressing the
-- runtime filter is correct - a merge join cannot consume one), for the inner and the outer kinds alike.
SELECT 'using_inner_merge_selected', countIf(explain LIKE '%Algorithm: FullSortingMergeJoin%') = 1 AND countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN actions = 1 SELECT * FROM smj_nulls_left AS l INNER JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash');

SELECT 'using_left_merge_selected', countIf(explain LIKE '%Algorithm: FullSortingMergeJoin%') = 1 AND countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN actions = 1 SELECT * FROM smj_nulls_left AS l LEFT JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash');

SELECT 'using_full_merge_selected', countIf(explain LIKE '%Algorithm: FullSortingMergeJoin%') = 1 AND countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN actions = 1 SELECT * FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash');

-- The in-order reads are really exploited.
SELECT 'using_full_reads_in_order', countIf(explain LIKE '%ReadType: InOrder%') = 2
FROM (EXPLAIN actions = 1 SELECT * FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash');

-- Correctness of `USING` + `join_use_nulls` against `hash` for every kind.
SELECT 'using_inner_result',
    (SELECT (sum(id), sum(a + b), count()) FROM smj_nulls_left AS l INNER JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(id), sum(a + b), count()) FROM smj_nulls_left AS l INNER JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'hash');

SELECT 'using_left_result',
    (SELECT (sum(id), sum(a), sum(b), count()) FROM smj_nulls_left AS l LEFT JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(id), sum(a), sum(b), count()) FROM smj_nulls_left AS l LEFT JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'hash');

SELECT 'using_full_result',
    (SELECT (sum(id), sum(a), sum(b), count()) FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(id), sum(a), sum(b), count()) FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'hash');

-- `parallel_sorted_merge` + `join_use_nulls`: the primary-key-range sharding still applies for the OUTER
-- kinds, where the visible output columns (including the keys) become `Nullable` - the conversion is
-- applied above the join and does not obscure the keys from the sharding matcher.
SELECT 'parallel_left_nulls_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT l.a FROM smj_nulls_left AS l LEFT JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

SELECT 'parallel_right_nulls_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT r.b FROM smj_nulls_left AS l RIGHT JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

SELECT 'parallel_full_nulls_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT l.a, r.b FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

SELECT 'parallel_using_full_nulls_sharded', countIf(explain LIKE '%Sharding:%') = 1
FROM (EXPLAIN actions = 1 SELECT * FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- Correctness of the sharded outer joins under `join_use_nulls` against `hash`.
SELECT 'parallel_left_nulls_result',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_nulls_left AS l LEFT JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_nulls_left AS l LEFT JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'parallel_right_nulls_result',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_nulls_left AS l RIGHT JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_nulls_left AS l RIGHT JOIN smj_nulls_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'parallel_using_full_nulls_result',
    (SELECT (sum(id), sum(a), sum(b), count()) FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(id), sum(a), sum(b), count()) FROM smj_nulls_left AS l FULL JOIN smj_nulls_right AS r USING (id) SETTINGS join_algorithm = 'hash');

DROP TABLE smj_nulls_left;
DROP TABLE smj_nulls_right;
