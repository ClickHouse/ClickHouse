-- A null-safe equality (`<=>`) over nullable keys is not readable in table order: physicalization wraps
-- both keys into `tuple(...)` and the merge join is sorted by the wrapped names, which the read-in-order
-- optimization cannot match. Such a join must therefore fall through to the next entry of
-- `join_algorithm` instead of selecting `sorted_merge` / `parallel_sorted_merge` and paying a full sort.
-- A `<=>` over non-nullable keys is a plain equality (no wrapping) and stays eligible.

DROP TABLE IF EXISTS smj_ns_left;
DROP TABLE IF EXISTS smj_ns_right;
DROP TABLE IF EXISTS smj_nn_left;
DROP TABLE IF EXISTS smj_nn_right;

CREATE TABLE smj_ns_left (id Nullable(UInt64), a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256, allow_nullable_key = 1;
CREATE TABLE smj_ns_right (id Nullable(UInt64), b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256, allow_nullable_key = 1;

INSERT INTO smj_ns_left SELECT if(number % 7 = 0, NULL, number % 3000), number FROM numbers(0, 4000);
INSERT INTO smj_ns_right SELECT if(number % 5 = 0, NULL, number % 2000), number * 2 FROM numbers(0, 5000);

CREATE TABLE smj_nn_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;
CREATE TABLE smj_nn_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 256;

INSERT INTO smj_nn_left SELECT number % 3000, number FROM numbers(0, 4000);
INSERT INTO smj_nn_right SELECT number % 2000, number * 2 FROM numbers(0, 5000);

SET enable_analyzer = 1;
-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- The nullable `<=>` join falls through to `hash`.
SELECT 'null_safe_falls_through_to_hash', countIf(explain LIKE '%Algorithm: HashJoin%') = 1 AND countIf(explain LIKE '%FullSortingMergeJoin%') = 0
FROM (EXPLAIN actions = 1 SELECT l.a, r.b FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'sorted_merge,hash');

SELECT 'null_safe_parallel_falls_through_to_hash', countIf(explain LIKE '%Algorithm: HashJoin%') = 1 AND countIf(explain LIKE '%FullSortingMergeJoin%') = 0
FROM (EXPLAIN actions = 1 SELECT l.a, r.b FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- `parallel_sorted_merge` does not shard such a join by primary-key ranges either.
SELECT 'null_safe_not_sharded', countIf(explain LIKE '%Sharding:%') = 0
FROM (EXPLAIN actions = 1 SELECT l.a, r.b FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- A `<=>` over non-nullable keys is an ordinary equality and still selects the merge join reading in order.
SELECT 'non_nullable_null_safe_merge_selected', countIf(explain LIKE '%Algorithm: FullSortingMergeJoin%') = 1 AND countIf(explain LIKE '%ReadType: InOrder%') = 2
FROM (EXPLAIN actions = 1 SELECT l.a, r.b FROM smj_nn_left AS l JOIN smj_nn_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'sorted_merge,hash');

-- Whichever algorithm is selected, the results match `hash`.
SELECT 'null_safe_result',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'hash');

SELECT 'null_safe_parallel_result',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_ns_left AS l JOIN smj_ns_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'hash');

SELECT 'non_nullable_null_safe_result',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_nn_left AS l JOIN smj_nn_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_nn_left AS l JOIN smj_nn_right AS r ON l.id <=> r.id SETTINGS join_algorithm = 'hash');

DROP TABLE smj_ns_left;
DROP TABLE smj_ns_right;
DROP TABLE smj_nn_left;
DROP TABLE smj_nn_right;
