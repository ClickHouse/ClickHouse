-- https://github.com/ClickHouse/ClickHouse/issues/120204
-- The plan-time join conversions decide by evaluating the filter over fabricated constant inputs. That
-- makes a per-row subexpression constant, so it takes the constant-NULL short circuit, which never
-- computes the value stored under the null map: `assumeNotNull` read the type default at plan time and
-- the computed value at runtime, and the conversions dropped rows the filter keeps.

-- The conversions are plan-level optimizations that the test harness turns off at random, so ask for
-- them explicitly - otherwise the positive control below is a coin flip. Parallel replicas build a
-- different plan, and the plan is what is asserted here.
SET query_plan_enable_optimizations = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET query_plan_merge_filter_into_join_condition = 0; -- absorbing WHERE into ON prevents the conversion
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_ann_left;
DROP TABLE IF EXISTS t_ann_right;
CREATE TABLE t_ann_left (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_ann_right (id UInt64, val2 Int64) ENGINE = MergeTree ORDER BY id;
-- No key matches, so every row reaches the filter only as a not-matched row of the outer side.
INSERT INTO t_ann_left SELECT number, number FROM numbers(5);
INSERT INTO t_ann_right SELECT number + 100, number FROM numbers(6);

-- RIGHT JOIN converted to INNER, the reported shape.
SELECT count() FROM t_ann_left RIGHT JOIN t_ann_right ON t_ann_left.id = t_ann_right.id
WHERE assumeNotNull(nullIf(t_ann_left.val, 0) + 42);

-- FULL JOIN is demoted to LEFT or RIGHT when one side looks safe, a differently sized wrong answer.
SELECT count() FROM t_ann_left FULL JOIN t_ann_right ON t_ann_left.id = t_ann_right.id
WHERE assumeNotNull(nullIf(t_ann_left.val, 0) + 42);

-- ANY LEFT JOIN converted to SEMI. The hazard has to be over the non-preserved side: that call site
-- leaves a predicate over the other side unbound.
SELECT count() FROM t_ann_left ANY LEFT JOIN t_ann_right ON t_ann_left.id = t_ann_right.id
WHERE assumeNotNull(nullIf(t_ann_right.val2, 0) + 42);

-- Inside a lambda the hazard lives in the lambda's own DAG, which a walk over this DAG does not reach.
SELECT count() FROM t_ann_left RIGHT JOIN t_ann_right ON t_ann_left.id = t_ann_right.id
WHERE arrayExists(x -> assumeNotNull(nullIf(t_ann_left.val, 0) + x) != 0, [42]);

-- Over a non-Nullable argument `assumeNotNull` is an identity, so the conversion must still happen.
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_ann_left RIGHT JOIN t_ann_right ON t_ann_left.id = t_ann_right.id
    WHERE assumeNotNull(t_ann_left.id) = 12345
) WHERE explain ILIKE '%Type: inner%';

-- Over a non-Nullable argument the conversion must still happen on the ANY path too. Without this the
-- ANY LEFT JOIN arm above would also pass in a plan where nothing is converted at all.
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_ann_left ANY LEFT JOIN t_ann_right ON t_ann_left.id = t_ann_right.id
    WHERE assumeNotNull(t_ann_right.val2) = 12345
) WHERE explain ILIKE '%Strictness: semi%';

DROP TABLE t_ann_left;
DROP TABLE t_ann_right;
