-- `sorted_merge` is a merge-join algorithm that is available only when both join inputs can be efficiently
-- read in the order of the join keys (here: MergeTree tables whose primary key is the join key), so the
-- pre-join sorts become cheap `FinishSorting` instead of full sorts. When the tables' order cannot be
-- exploited - the join key is not a primary-key prefix, reading in order is disabled, or the old analyzer
-- is used - the algorithm is NOT selected and the priority list falls through to the next entry. This is
-- the difference from `full_sorting_merge`, which is always available and therefore shadows anything listed
-- after it.

DROP TABLE IF EXISTS smj_left;
DROP TABLE IF EXISTS smj_right;

CREATE TABLE smj_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE smj_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

-- Several parts per side so each read produces multiple in-order streams; duplicate ids exercise
-- many-to-many matches.
INSERT INTO smj_left SELECT number % 30000, number FROM numbers(0, 40000);
INSERT INTO smj_left SELECT number % 30000, number FROM numbers(40000, 40000);
INSERT INTO smj_right SELECT number % 20000, number * 2 FROM numbers(0, 50000);
INSERT INTO smj_right SELECT number % 20000, number * 3 FROM numbers(50000, 50000);

-- The eligibility of `sorted_merge` is decided on the query plan, which exists only for the analyzer.
-- The default is overridden to 0 in the old-analyzer CI configuration, so pin it explicitly.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on: the in-order read must be allowed
-- (`optimize_read_in_order`, `query_plan_read_in_order`), the PK-range sharding stays out of the picture
-- (`query_plan_join_shard_by_pk_ranges`), sides are not swapped (`query_plan_join_swap_table`), and the
-- reads are local (`enable_parallel_replicas`).
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

-- The join is on `id`, the primary key of both sides: `sorted_merge` is eligible and, being first in the
-- list, selected - the pipeline runs a merge join.
SELECT 'sorted_selected_merge', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- ...and it must not sort from scratch: the in-order sides only merge, no `MergeSortingTransform`.
SELECT 'sorted_not_resorted', countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- The join is on `a`, not a primary-key prefix: `sorted_merge` is not eligible and the selection falls
-- through to `hash` - no merge join in the pipeline.
SELECT 'unsorted_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.id FROM smj_left AS l INNER JOIN smj_right AS r ON l.a = r.b SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- With the in-order read disabled the tables' order cannot be exploited, so `sorted_merge` is not eligible
-- even on the primary-key join.
SELECT 'read_in_order_off_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4, optimize_read_in_order = 0);

-- `join_algorithm` is an ordered priority list: with `hash` first, `hash` wins even though `sorted_merge`
-- is eligible.
SELECT 'priority_order_respected', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash,sorted_merge', max_threads = 4);

-- `sorted_merge` alone works on the primary-key join...
SELECT 'alone_on_sorted', sum(l.a + r.b) > 0, count() > 0 FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge';

-- ...but errors out when the order cannot be exploited and there is nothing to fall back to.
SELECT l.id FROM smj_left AS l INNER JOIN smj_right AS r ON l.a = r.b SETTINGS join_algorithm = 'sorted_merge'; -- { serverError NOT_IMPLEMENTED }

-- Correctness against `hash` for every join kind (the pipeline checks above pin that the merge algorithm
-- really is the one executing).
SELECT 'inner',
    (SELECT (sum(l.a + r.b), count()) FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a + r.b), count()) FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'left',
    (SELECT (sum(l.a), count()) FROM smj_left AS l LEFT JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a), count()) FROM smj_left AS l LEFT JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'right',
    (SELECT (sum(r.b), count()) FROM smj_left AS l RIGHT JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(r.b), count()) FROM smj_left AS l RIGHT JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_left AS l FULL JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_left AS l FULL JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'any_left',
    (SELECT (sum(l.a), count()) FROM smj_left AS l ANY LEFT JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a), count()) FROM smj_left AS l ANY LEFT JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT 'full_use_nulls',
    (SELECT (sum(l.a), sum(r.b), count()) FROM smj_left AS l FULL JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', join_use_nulls = 1)
  = (SELECT (sum(l.a), sum(r.b), count()) FROM smj_left AS l FULL JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash', join_use_nulls = 1);

-- The old analyzer has no query plan at selection time, so `sorted_merge` is never selected there: the
-- list falls through to `hash` even on the primary-key join, and `sorted_merge` alone errors out.
SET enable_analyzer = 0;

SELECT 'legacy_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

SELECT 'legacy_result',
    (SELECT (sum(l.a + r.b), count()) FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a + r.b), count()) FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash');

SELECT l.a FROM smj_left AS l INNER JOIN smj_right AS r ON l.id = r.id SETTINGS join_algorithm = 'sorted_merge'; -- { serverError NOT_IMPLEMENTED }

DROP TABLE smj_left;
DROP TABLE smj_right;
