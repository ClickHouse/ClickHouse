-- Under `join_use_nulls = 1` the right key of a `LEFT` join is `toNullable(x)` when the query also selects
-- `x` (`preferNullableRightKey`). When the right input is a nested merge join, it still emits its rows sorted
-- by `x`, so the join above it stays a merge join with `join_algorithm = 'sorted_merge,hash'`, with no
-- fall-through to `hash` and no re-sort.

DROP TABLE IF EXISTS mwjn_a;
DROP TABLE IF EXISTS mwjn_b;
DROP TABLE IF EXISTS mwjn_c;

CREATE TABLE mwjn_a (id UInt64, va UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mwjn_b (id UInt64, vb UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE mwjn_c (id UInt64, vc UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO mwjn_a SELECT number, number FROM numbers(0, 20000);
INSERT INTO mwjn_a SELECT number, number * 2 FROM numbers(20000, 20000);
INSERT INTO mwjn_b SELECT number % 30000, number FROM numbers(0, 45000);
INSERT INTO mwjn_c SELECT number * 2, number FROM numbers(0, 25000);

SET enable_analyzer = 1;
-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0, enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET max_threads = 4;
SET join_use_nulls = 1;

SELECT 'left_over_inner', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%FillingRightJoinSide%') = 0, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT c.id, s.id, sum(vc) FROM mwjn_c AS c LEFT JOIN (SELECT a.id AS id, va + vb AS v FROM mwjn_a AS a INNER JOIN mwjn_b AS b ON a.id = b.id) AS s ON c.id = s.id GROUP BY ALL SETTINGS join_algorithm = 'sorted_merge,hash');

SELECT 'left_over_right_using', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%FillingRightJoinSide%') = 0, countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT c.id, s.id, sum(vc) FROM mwjn_c AS c LEFT JOIN (SELECT id, va + vb AS v FROM mwjn_a AS a RIGHT JOIN mwjn_b AS b USING (id)) AS s ON c.id = s.id GROUP BY ALL SETTINGS join_algorithm = 'sorted_merge,hash');

-- Correctness against `hash`.
SELECT 'left_over_inner_result', count(), sum(c_id), sum(s_id), sum(v), sum(vc), countIf(s_id IS NULL)
FROM (SELECT c.id AS c_id, s.id AS s_id, v, vc FROM mwjn_c AS c LEFT JOIN (SELECT a.id AS id, va + vb AS v FROM mwjn_a AS a INNER JOIN mwjn_b AS b ON a.id = b.id) AS s ON c.id = s.id SETTINGS join_algorithm = 'sorted_merge,hash');
SELECT 'left_over_inner_result', count(), sum(c_id), sum(s_id), sum(v), sum(vc), countIf(s_id IS NULL)
FROM (SELECT c.id AS c_id, s.id AS s_id, v, vc FROM mwjn_c AS c LEFT JOIN (SELECT a.id AS id, va + vb AS v FROM mwjn_a AS a INNER JOIN mwjn_b AS b ON a.id = b.id) AS s ON c.id = s.id SETTINGS join_algorithm = 'hash');

SELECT 'left_over_right_using_result', count(), sum(c_id), sum(s_id), sum(v), sum(vc), countIf(s_id IS NULL), countIf(v IS NULL)
FROM (SELECT c.id AS c_id, s.id AS s_id, v, vc FROM mwjn_c AS c LEFT JOIN (SELECT id, va + vb AS v FROM mwjn_a AS a RIGHT JOIN mwjn_b AS b USING (id)) AS s ON c.id = s.id SETTINGS join_algorithm = 'sorted_merge,hash');
SELECT 'left_over_right_using_result', count(), sum(c_id), sum(s_id), sum(v), sum(vc), countIf(s_id IS NULL), countIf(v IS NULL)
FROM (SELECT c.id AS c_id, s.id AS s_id, v, vc FROM mwjn_c AS c LEFT JOIN (SELECT id, va + vb AS v FROM mwjn_a AS a RIGHT JOIN mwjn_b AS b USING (id)) AS s ON c.id = s.id SETTINGS join_algorithm = 'hash');

DROP TABLE mwjn_a;
DROP TABLE mwjn_b;
DROP TABLE mwjn_c;
