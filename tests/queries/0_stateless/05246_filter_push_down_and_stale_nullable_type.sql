-- Filter push-down can take a Nullable conjunct out of an `and` and leave the surviving `and`
-- still declaring the Nullable(UInt8) result type it had while that conjunct was an argument.
-- A distributed plan re-derives and checks the declared type of every function it reads back,
-- so such a query was rejected with INCORRECT_DATA instead of running.
-- Every query below is run twice, once with the distributed plan and push-down on and once with
-- both off, and the two runs must produce the same result.
-- Closes https://github.com/ClickHouse/ClickHouse/issues/121872

DROP TABLE IF EXISTS t_and_type_l;
DROP TABLE IF EXISTS t_and_type_r;
DROP TABLE IF EXISTS t_and_type_r2;
DROP TABLE IF EXISTS t_and_type_j;

CREATE TABLE t_and_type_l (number UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY number;
CREATE TABLE t_and_type_r (number UInt64) ENGINE = MergeTree ORDER BY number;
CREATE TABLE t_and_type_r2 (number UInt64) ENGINE = MergeTree ORDER BY number;
-- `m` is deliberately not the join key: a predicate on the key alone can be moved to either side
-- through the key equality, which hides whether the surviving conjunction was split again.
CREATE TABLE t_and_type_j (number UInt64, m UInt64) ENGINE = MergeTree ORDER BY number;

INSERT INTO t_and_type_l SELECT number, if(number = 5, NULL, number) FROM numbers(10);
INSERT INTO t_and_type_r SELECT number FROM numbers(10);
INSERT INTO t_and_type_r2 SELECT number FROM numbers(10);
INSERT INTO t_and_type_j SELECT number, number FROM numbers(10);

SET enable_parallel_replicas = 0;
-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0: randomized
-- settings set it nonzero, which makes the aggregations below fall back to local execution.
SET max_rows_to_group_by = 0;
-- The single-table conjunct is pushed as far as PREWHERE; either of these off stops it earlier and
-- does not exercise the split.
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, query_plan_filter_push_down = 1;

-- P0: every other row compares values against a push-down-off twin, so all of them would keep
-- passing if the optimizer ever stopped splitting this shape and the retyping under test stopped
-- running. This row pins the split itself: the Nullable conjunct must leave the filter above the
-- join and reach PREWHERE. The `plain` twin is the same query with push-down off, so the two lines
-- together show the row can report either outcome. The second counter excludes PREWHERE lines
-- explicitly, so the two counters stay disjoint whatever the plan printer does to its spacing.
SELECT 'P0 plan',
       countIf(explain ILIKE '%Prewhere filter column:%n > 0%'),
       countIf(explain ILIKE '%Filter column: n > 0%' AND explain NOT ILIKE '%Prewhere%')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
    WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 1);
SELECT 'P0 plain',
       countIf(explain ILIKE '%Prewhere filter column:%n > 0%'),
       countIf(explain ILIKE '%Filter column: n > 0%' AND explain NOT ILIKE '%Prewhere%')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
    WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0);

-- P1: the reported query. One Nullable single-table conjunct is pushed down, two joint conjuncts
-- remain, so an `and` survives with fewer arguments than it was typed for.
SELECT 'P1 dist', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9;
SELECT 'P1 plain', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- P2: the same split behind an equality LEFT JOIN.
SELECT 'P2 dist', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l LEFT JOIN t_and_type_r AS r ON l.number = r.number
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9;
SELECT 'P2 plain', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l LEFT JOIN t_and_type_r AS r ON l.number = r.number
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- P3: both join sides are subqueries, so the conjunct is pushed through one more level.
SELECT 'P3 dist', count(), sum(l.number), sum(r.number)
FROM (SELECT number, n FROM t_and_type_l) AS l CROSS JOIN (SELECT number FROM t_and_type_r) AS r
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9;
SELECT 'P3 plain', count(), sum(l.number), sum(r.number)
FROM (SELECT number, n FROM t_and_type_l) AS l CROSS JOIN (SELECT number FROM t_and_type_r) AS r
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- P4: the split happens inside a subquery that is itself aggregated.
SELECT 'P4 dist', count(), sum(s)
FROM (
    SELECT l.number AS k, sum(r.number) AS s
    FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
    WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
    GROUP BY k);
SELECT 'P4 plain', count(), sum(s)
FROM (
    SELECT l.number AS k, sum(r.number) AS s
    FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
    WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
    GROUP BY k)
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- P5: three conjuncts survive the split instead of two.
SELECT 'P5 dist', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9 AND l.number + r.number != 5;
SELECT 'P5 plain', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9 AND l.number + r.number != 5
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- P6: the filter column is also selected, so it must keep the Nullable(UInt8) it is declared with.
-- Reconciling the type by simply adopting the surviving arguments' type would report UInt8 here.
SELECT 'P6 dist', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9) AS c
GROUP BY c ORDER BY c;
SELECT 'P6 plain', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- P7: the shortened conjunction must remain a conjunction rather than become one opaque value, or a
-- later pass can no longer split it. The filter above this LEFT JOIN keeps two conjuncts; the join
-- then becomes an inner join because both of them reject the NULL-extended rows, and the filter is
-- offered for splitting a second time, which is the only pass that can push `j.m > 3`. The other
-- optimizations are off because each is a separate route to the same plan and would mask this one.
SELECT 'P7 plan', countIf(explain ILIKE '%Prewhere filter column:%m > 3%')
FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
    WHERE l.n > 0 AND j.m > 3 AND l.n + j.m < 15
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 1,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 0,
             query_plan_propagate_predicate_across_join = 0,
             enable_join_runtime_filters = 0);

-- P8: the same second split, with the filter column retained and every conjunct non-nullable, so
-- the surviving conjunction already carries the type the whole condition was declared with. Nothing
-- has to be restored here, and so nothing may be placed above the conjunction either: `j.m > 3` must
-- still reach PREWHERE. The `plain` twin differs in `query_plan_filter_push_down` alone, so the two
-- lines together show the row can report either outcome. Unlike P7 this shape reaches the second
-- split only once the two filters are merged, and a join runtime filter is a second route to the
-- same PREWHERE line, so both rows pin those too, along with P7's other settings.
SELECT 'P8 plan', countIf(explain ILIKE '%Prewhere filter column:%m > 3%')
FROM (
    EXPLAIN actions = 1
    SELECT c, count() FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
    WHERE (l.number > 0 AND j.m > 3 AND l.number + j.m < 15) AS c
    GROUP BY c
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 1,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 0,
             query_plan_propagate_predicate_across_join = 0,
             enable_join_runtime_filters = 0, query_plan_merge_filters = 1);
SELECT 'P8 plain', countIf(explain ILIKE '%Prewhere filter column:%m > 3%')
FROM (
    EXPLAIN actions = 1
    SELECT c, count() FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
    WHERE (l.number > 0 AND j.m > 3 AND l.number + j.m < 15) AS c
    GROUP BY c
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 0,
             query_plan_propagate_predicate_across_join = 0,
             enable_join_runtime_filters = 0, query_plan_merge_filters = 1);

-- P9: P8's shape with the pushed-out conjunct Nullable, so the surviving conjunction does not carry
-- the declared type and it has to be restored. It has to be restored on one argument of the
-- conjunction rather than above it, or `j.m > 3` no longer reaches PREWHERE once the join becomes an
-- inner one. Same twin and the same pinned settings as P8, and the second counter again excludes
-- PREWHERE lines so that the two counters stay disjoint.
SELECT 'P9 plan',
       countIf(explain ILIKE '%Prewhere filter column:%m > 3%'),
       countIf(explain ILIKE '%Filter column:%m > 3%' AND explain NOT ILIKE '%Prewhere%')
FROM (
    EXPLAIN actions = 1
    SELECT c, count() FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
    WHERE (l.n > 0 AND j.m > 3 AND l.number + j.m < 15) AS c
    GROUP BY c
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 1,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 0,
             query_plan_propagate_predicate_across_join = 0,
             enable_join_runtime_filters = 0, query_plan_merge_filters = 1);
SELECT 'P9 plain',
       countIf(explain ILIKE '%Prewhere filter column:%m > 3%'),
       countIf(explain ILIKE '%Filter column:%m > 3%' AND explain NOT ILIKE '%Prewhere%')
FROM (
    EXPLAIN actions = 1
    SELECT c, count() FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
    WHERE (l.n > 0 AND j.m > 3 AND l.number + j.m < 15) AS c
    GROUP BY c
    SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0,
             query_plan_convert_outer_join_to_inner_join = 1,
             query_plan_merge_filter_into_join_condition = 0,
             query_plan_propagate_predicate_across_join = 0,
             enable_join_runtime_filters = 0, query_plan_merge_filters = 1);

-- P9b: P9's condition is rewritten twice before it reaches that plan, once when the declared type
-- moves onto an argument and once when a single argument is left, so read its values and type back.
SELECT 'P9b plan', c, toTypeName(c), count()
FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
WHERE (l.n > 0 AND j.m > 3 AND l.number + j.m < 15) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 1,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 0,
         query_plan_propagate_predicate_across_join = 0,
         enable_join_runtime_filters = 0, query_plan_merge_filters = 1;
SELECT 'P9b plain', c, toTypeName(c), count()
FROM t_and_type_l AS l LEFT JOIN t_and_type_j AS j ON l.number = j.number
WHERE (l.n > 0 AND j.m > 3 AND l.number + j.m < 15) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0,
         query_plan_convert_outer_join_to_inner_join = 1,
         query_plan_merge_filter_into_join_condition = 0,
         query_plan_propagate_predicate_across_join = 0,
         enable_join_runtime_filters = 0, query_plan_merge_filters = 1;

-- N1: the Nullable conjunct is joint, so it stays and the surviving `and` is Nullable(UInt8) for
-- real. The declared type must be left alone when it did not change.
SELECT 'N1 dist', count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.number > 2 AND l.n + r.number > 1 AND l.number + r.number < 15;
SELECT 'N1 plain', count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.number > 2 AND l.n + r.number > 1 AND l.number + r.number < 15
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N1b: N1 keeps no filter column, so the type it is about is not observable in it. This is the same
-- shape with the condition selected, which is where Nullable(UInt8) can actually be read back.
SELECT 'N1b dist', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.number > 2 AND l.n + r.number > 1 AND l.number + r.number < 15) AS c
GROUP BY c ORDER BY c;
SELECT 'N1b plain', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.number > 2 AND l.n + r.number > 1 AND l.number + r.number < 15) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N2: an equality INNER JOIN distributes the joint conjuncts per side, so no `and` survives above
-- the join.
SELECT 'N2 dist', count()
FROM t_and_type_l AS l INNER JOIN t_and_type_r AS r ON l.number = r.number
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9;
SELECT 'N2 plain', count()
FROM t_and_type_l AS l INNER JOIN t_and_type_r AS r ON l.number = r.number
WHERE l.n > 0 AND l.number + r.number > 1 AND l.number + r.number < 9
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N3: a single table, where the whole filter is pushed and nothing survives to be retyped.
SELECT 'N3 dist', count()
FROM t_and_type_l AS l
WHERE l.n > 0 AND l.number > 1 AND l.number < 9;
SELECT 'N3 plain', count()
FROM t_and_type_l AS l
WHERE l.n > 0 AND l.number > 1 AND l.number < 9
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N4: three-way join with the joint conjuncts on different table pairs, so each join level keeps at
-- most one of them.
SELECT 'N4 dist', count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r CROSS JOIN t_and_type_r2 AS r2
WHERE l.n > 0 AND l.number + r.number > 1 AND r.number + r2.number < 9;
SELECT 'N4 plain', count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r CROSS JOIN t_and_type_r2 AS r2
WHERE l.n > 0 AND l.number + r.number > 1 AND r.number + r2.number < 9
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N5: the Nullable conjunct is the whole filter, so there is no `and` at all.
SELECT 'N5 dist', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0;
SELECT 'N5 plain', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N6: exactly one conjunct survives, which is replaced by that conjunct rather than by a smaller
-- `and`.
SELECT 'N6 dist', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0 AND l.number + r.number > 1;
SELECT 'N6 plain', count(), sum(l.number), sum(r.number)
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE l.n > 0 AND l.number + r.number > 1
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N7: `and` is declared Bool as soon as any argument is Bool, so here the whole condition and its one
-- surviving conjunct are both Bool and the reported type cannot tell them apart. It is a value
-- control for the single-survivor path, not a type discriminator; N9 is the discriminating shape.
SELECT 'N7 dist', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.number > 0 AND (l.number + r.number > 1)::Bool) AS c
GROUP BY c ORDER BY c;
SELECT 'N7 plain', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.number > 0 AND (l.number + r.number > 1)::Bool) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N8: the same with a LowCardinality(UInt8) survivor.
SELECT 'N8 dist', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.n > 0 AND toLowCardinality(l.number + r.number > 1)) AS c
GROUP BY c ORDER BY c;
SELECT 'N8 plain', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (l.n > 0 AND toLowCardinality(l.number + r.number > 1)) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N9: the Bool conjunct is the one pushed OUT, so the surviving conjuncts are plain UInt8 while the
-- whole condition is declared Bool. This is the only arrangement where the declared type and the
-- surviving arguments' type disagree in that direction, so it is what checks that the declared type
-- is restored rather than recomputed.
SELECT 'N9 dist', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE ((l.number > 0)::Bool AND l.number + r.number > 1 AND l.number + r.number < 9) AS c
GROUP BY c ORDER BY c;
SELECT 'N9 plain', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE ((l.number > 0)::Bool AND l.number + r.number > 1 AND l.number + r.number < 9) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

-- N10: the same arrangement with a LowCardinality(UInt8) conjunct pushed out. `and` reports plain
-- UInt8 for it, so unlike N9 the declared type and the survivors' type agree and there is nothing to
-- restore; the row is here to hold that normalization still in place.
SELECT 'N10 dist', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (toLowCardinality(l.number > 0) AND l.number + r.number > 1 AND l.number + r.number < 9) AS c
GROUP BY c ORDER BY c;
SELECT 'N10 plain', c, toTypeName(c), count()
FROM t_and_type_l AS l CROSS JOIN t_and_type_r AS r
WHERE (toLowCardinality(l.number > 0) AND l.number + r.number > 1 AND l.number + r.number < 9) AS c
GROUP BY c ORDER BY c
SETTINGS make_distributed_plan = 0, query_plan_filter_push_down = 0;

DROP TABLE t_and_type_l;
DROP TABLE t_and_type_r;
DROP TABLE t_and_type_r2;
DROP TABLE t_and_type_j;
