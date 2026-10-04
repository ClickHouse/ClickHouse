-- A join with no condition at all is a cross product: the two sides are joined on nothing, so the
-- query graph they form is disconnected. DPsub is built for connected graphs and cannot stitch the
-- components, so it declines such a query and the next algorithm in the chain plans it. That is
-- what already happened without a conflict detector; with one enabled, a connectivity link used to
-- be seeded for the cross product too, which let DPsub enumerate a graph it is not built for and
-- report the join as `INNER`. The kind is what `applyParallelReplicas` and the join columns of
-- `system.query_log` go by, so the mislabelling was user visible.
-- The transitive case is the contrast: two sides tied only by a column equivalence, with no direct
-- predicate, form a connected graph that DPsub does plan, and it stays `INNER`.

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

-- DPsub on its own has nothing to plan here and says so, with or without a detector. This is the
-- behaviour the fix restores for the detector cases: before it, `'dpsub'` alone answered `cross`
-- for a graph it should have turned down.
SELECT '-- dpsub alone declines an unconditioned join';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_conflict_detector = 'c'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

-- A nested cross product: `t_05238_a` is attached to the rest only by the cross join, even though
-- the inner join above it has a predicate. The inner operator's predicate spans `b` and `c` only,
-- so it must not count as a link to `a`, and DPsub still has to decline the graph.
SELECT '-- dpsub alone declines a nested cross product';
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b JOIN t_05238_c ON t_05238_b.a = t_05238_c.a
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_conflict_detector = 'a'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b JOIN t_05238_c ON t_05238_b.a = t_05238_c.a
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_conflict_detector = 'c'; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

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
