-- A join with no condition at all is a cross product: the two sides are joined on nothing. The query
-- graph links the two sides of every cross product, as a join of selectivity 1, so a graph that cross
-- products hold together is connected and DPsub plans it, with or without a conflict detector. The
-- join it ends up as still has no condition, so it must be reported as `CROSS`, not `INNER`: the kind
-- is what `applyParallelReplicas` and the join columns of `system.query_log` go by, so a mislabelling
-- would be user visible.
-- The transitive case is the contrast: two sides tied only by a column equivalence, with no direct
-- predicate, are a real join, and it stays `INNER`.

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
-- The harness randomizes the limit, and at 0 (or below the number of joined tables) the join order
-- algorithms do not run at all, so nothing here would exercise the path under test.
SET query_plan_optimize_join_order_limit = 10;

-- DPsub on its own plans an unconditioned join, with or without a detector, and keeps it `CROSS`.
SELECT '-- dpsub alone plans an unconditioned join';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%';

-- A nested cross product: `t_05238_a` is attached to the rest only by the cross join, even though the
-- inner join above it has a predicate. The cross product links it, so DPsub plans the query, and the
-- join that brings in `t_05238_a` stays `CROSS`.
SELECT '-- dpsub alone plans a nested cross product';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b JOIN t_05238_c ON t_05238_b.a = t_05238_c.a
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'a';
SELECT extract(explain, 'Type: [a-z]+') AS kind FROM (
    EXPLAIN actions = 1 SELECT count() FROM t_05238_a CROSS JOIN t_05238_b JOIN t_05238_c ON t_05238_b.a = t_05238_c.a
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%' ORDER BY kind;

SELECT '-- nested cross product keeps its kind, CD-C';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a CROSS JOIN t_05238_b JOIN t_05238_c ON t_05238_b.a = t_05238_c.a
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
             query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b JOIN t_05238_c ON t_05238_b.a = t_05238_c.a
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
         query_plan_optimize_join_order_conflict_detector = 'c';

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

-- Connected, so DPsub plans it on its own and must keep the kind.
SELECT '-- transitive inner join stays inner, CD-C';
SELECT extract(explain, 'Type: [a-z]+') FROM (
    EXPLAIN SELECT count() FROM t_05238_a, t_05238_b, t_05238_c
    WHERE t_05238_a.a = t_05238_b.a AND t_05238_b.a = t_05238_c.a
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
             query_plan_optimize_join_order_conflict_detector = 'c'
) WHERE explain LIKE '%Type:%';

-- The reported kind is what the join columns of `system.query_log` are built from, and `EXPLAIN`
-- never reaches that path, so assert on an executed query as well.
SELECT '-- query_log reports CROSS, CD-C';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
FORMAT Null
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
         query_plan_optimize_join_order_conflict_detector = 'c',
         log_comment = '05238_cross_cdc';

SYSTEM FLUSH LOGS query_log;
SELECT used_join_kinds
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment = '05238_cross_cdc';

SELECT '-- query_log reports INNER for a real inner join, CD-C';
SELECT count() FROM t_05238_a INNER JOIN t_05238_b ON t_05238_a.a = t_05238_b.a
FORMAT Null
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_conflict_detector = 'c',
         log_comment = '05238_inner_cdc';

SYSTEM FLUSH LOGS query_log;
SELECT used_join_kinds
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment = '05238_inner_cdc';

SELECT '-- results are unaffected';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub,greedy',
         query_plan_optimize_join_order_conflict_detector = 'c';

DROP TABLE t_05238_a;
DROP TABLE t_05238_b;
DROP TABLE t_05238_c;
