-- A `WHERE` equality is merged into the JOIN condition when its operands have differing
-- types but a common supertype, e.g. `Int32` and `Nullable(Int32)`.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET allow_experimental_correlated_subqueries = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_join_swap_table = 'false'; -- Ensure join conditions appear in the same order as the query
SET enable_join_runtime_filters = 0; -- Ensure all filters in the plan are from the query
SET query_plan_optimize_join_order_limit = 10;

CREATE TABLE t1 (a Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (b Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (c UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tj (a Int32, d Nullable(Int32)) ENGINE = Join(ALL, INNER, a);

INSERT INTO t1 VALUES (1), (2), (3);
INSERT INTO t2 VALUES (1), (NULL), (2);
INSERT INTO t3 VALUES (1), (2);
INSERT INTO tj VALUES (1, 1), (2, 5);

SELECT '-- `Int32` = `Nullable(Int32)` has common supertype `Nullable(Int32)`';
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain LIKE '%Filter column:%') AS filters_above_join
FROM (
    EXPLAIN SELECT * FROM (SELECT * FROM t1 INNER JOIN t2 ON 1) WHERE a = b
);

SELECT * FROM (SELECT * FROM t1 INNER JOIN t2 ON 1) WHERE a = b ORDER BY ALL;

SELECT '-- `Int32` = `UInt64` has no common supertype';
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain LIKE '%Filter column:%') AS filters_above_join
FROM (
    EXPLAIN SELECT * FROM (SELECT * FROM t1 INNER JOIN t3 ON 1) WHERE a = c
);

SELECT * FROM (SELECT * FROM t1 INNER JOIN t3 ON 1) WHERE a = c ORDER BY ALL;

SELECT '-- Conditions are not merged into a `Join` table engine';
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    countIf(explain LIKE '%Filter column:%') AS filters_above_join
FROM (
    EXPLAIN SELECT * FROM t1 ALL INNER JOIN tj ON t1.a = tj.a WHERE t1.a = tj.d
);

SELECT * FROM t1 ALL INNER JOIN tj ON t1.a = tj.a WHERE t1.a = tj.d ORDER BY ALL;

-- Disabled so the correlated subquery decorrelates via the CROSS JOIN this section verifies,
-- instead of being planned away by equivalent-expression substitution.
SET correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT '-- CROSS JOIN from correlated subquery with condition `Int32` = `Nullable(Int32)`';
SELECT
    countIf(explain LIKE '%Type: cross%') AS cross_joins,
    countIf(explain LIKE '%Join conditions:%') AS joins_with_conditions
FROM (
    EXPLAIN SELECT a, (SELECT count() FROM t2 WHERE t2.b = t1.a) FROM t1
);

SELECT a, (SELECT count() FROM t2 WHERE t2.b = t1.a) AS matches FROM t1 ORDER BY a;
