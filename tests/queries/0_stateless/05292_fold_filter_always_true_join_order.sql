-- a folded always-true filter must not split the join graph: one join step for all three tables
CREATE TABLE jt_a (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE jt_b (k UInt64, d UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE jt_c (d UInt64) ENGINE = MergeTree ORDER BY d;
INSERT INTO jt_a SELECT number FROM numbers(1000);
INSERT INTO jt_b SELECT number % 1000, number % 100 FROM numbers(100000);
INSERT INTO jt_c SELECT number FROM numbers(100);

SET query_plan_optimize_join_order_limit = 10, enable_parallel_replicas = 0;

SELECT countIf(explain LIKE '%⋈%⋈%')
FROM (EXPLAIN SELECT count() FROM jt_a, jt_b, jt_c WHERE jt_a.k = jt_b.k AND jt_b.d = jt_c.d AND materialize('w') = 'w');
SELECT count() FROM jt_a, jt_b, jt_c WHERE jt_a.k = jt_b.k AND jt_b.d = jt_c.d AND materialize('w') = 'w';
