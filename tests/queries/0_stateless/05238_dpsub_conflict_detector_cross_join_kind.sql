-- A join with no condition at all is a cross product, and the plan has to say so: the kind is what
-- `applyParallelReplicas` and the join observability columns go by. DPsub with a conflict detector
-- used to report it as `INNER`, because a cross product shares its reordering category with inner
-- joins and the kind was left at the `Inner` the resolver starts from.
-- The transitive case is here for the opposite reason: two sides tied only by a column equivalence,
-- with no direct predicate, are a real inner join and must not be turned into a cross product.

DROP TABLE IF EXISTS t_05238_a;
DROP TABLE IF EXISTS t_05238_b;
DROP TABLE IF EXISTS t_05238_c;

CREATE TABLE t_05238_a (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_05238_b (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_05238_c (a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_05238_a SELECT number FROM numbers(4);
INSERT INTO t_05238_b SELECT number FROM numbers(3);
INSERT INTO t_05238_c SELECT number FROM numbers(3);

SET query_plan_optimize_join_order_randomize = 0; -- the test asserts on the join kind

SELECT '-- cross join, no detector';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy'
) WHERE explain LIKE '%Type:%';

SELECT '-- cross join, CD-A';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
             query_plan_optimize_join_order_conflict_detector = 'a'
) WHERE explain LIKE '%Type:%';

SELECT '-- cross join, CD-C';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
             query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%';

SELECT '-- comma join with no predicate, CD-C';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a, t_05238_b
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
             query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%';

SELECT '-- transitive inner join stays inner, CD-C';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a, t_05238_b, t_05238_c
    WHERE t_05238_a.a = t_05238_b.a AND t_05238_b.a = t_05238_c.a
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
             query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%';

SELECT '-- results are unaffected';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
         query_plan_optimize_join_order_conflict_detector = 'c';

DROP TABLE t_05238_a;
DROP TABLE t_05238_b;
DROP TABLE t_05238_c;
