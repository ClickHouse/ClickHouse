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

-- DPsub on its own has nothing to plan here and says so, with or without a detector. This is the
-- behaviour the fix restores for the detector cases: before it, `'dpsub'` alone answered `cross`
-- for a graph it should have turned down.
SELECT '-- dpsub alone declines an unconditioned join';
-- `query_plan_optimize_join_order_limit` is pinned because the harness randomizes it, and at 0 no
-- algorithm runs at all, so nothing would decline and the query would simply succeed.
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_limit = 10; -- { serverError EXPERIMENTAL_FEATURE_ERROR }
SELECT count() FROM t_05238_a CROSS JOIN t_05238_b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub',
         query_plan_optimize_join_order_conflict_detector = 'c',
         query_plan_optimize_join_order_limit = 10; -- { serverError EXPERIMENTAL_FEATURE_ERROR }

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
