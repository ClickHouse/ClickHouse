-- The merge join of an `ASOF` join is sorted by the equality keys first and the inequality key last,
-- whatever the order the predicates were written in `ON`: physicalization materializes every equality
-- key of the clause and only then appends the `ASOF` key. The eligibility probe of `sorted_merge` and
-- `parallel_sorted_merge` must use that same order, so that the selection depends on the join semantics
-- and not on the textual order of the `ON` predicates.

DROP TABLE IF EXISTS smj_asof_left;
DROP TABLE IF EXISTS smj_asof_right;

CREATE TABLE smj_asof_left (id UInt64, ts UInt64, a UInt64) ENGINE = MergeTree ORDER BY (id, ts);
CREATE TABLE smj_asof_right (id UInt64, ts UInt64, b UInt64) ENGINE = MergeTree ORDER BY (id, ts);

INSERT INTO smj_asof_left SELECT number % 500, number, number FROM numbers(0, 5000);
INSERT INTO smj_asof_right SELECT number % 500, number * 2, number * 3 FROM numbers(0, 5000);

-- The eligibility is decided on the query plan, which exists only for the analyzer.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, enable_parallel_replicas = 0;

-- The equality written first: the probe order `(id, ts)` matches the tables' primary key either way.
SELECT 'equality_first_selected', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.id = r.id AND l.ts >= r.ts SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

SELECT 'equality_first_not_resorted', countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.id = r.id AND l.ts >= r.ts SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- The same join with the inequality written first. The merge-join sort key is still `(id, ts)`, so the
-- algorithm must be selected here too - reading the predicates in `ON` order would probe `(ts, id)` and
-- fall through to `hash`.
SELECT 'inequality_first_selected', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.ts >= r.ts AND l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

SELECT 'inequality_first_not_resorted', countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.ts >= r.ts AND l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

SELECT 'inequality_first_parallel_selected', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.ts >= r.ts AND l.id = r.id SETTINGS join_algorithm = 'parallel_sorted_merge,hash', max_threads = 4);

-- The result does not depend on the order of the predicates either.
SELECT 'inequality_first_result',
    (SELECT (sum(l.a + r.b), count()) FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.ts >= r.ts AND l.id = r.id SETTINGS join_algorithm = 'sorted_merge,hash')
  = (SELECT (sum(l.a + r.b), count()) FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.id = r.id AND l.ts >= r.ts SETTINGS join_algorithm = 'hash');

-- A join whose keys really are in the wrong order for the tables still falls through: `(ts, id)` is not a
-- primary-key prefix of `(id, ts)`, so nothing here makes the probe unconditionally optimistic.
SELECT 'wrong_key_order_falls_through', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT l.a FROM smj_asof_left AS l ASOF JOIN smj_asof_right AS r ON l.ts = r.ts AND l.id >= r.id SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

DROP TABLE smj_asof_left;
DROP TABLE smj_asof_right;
