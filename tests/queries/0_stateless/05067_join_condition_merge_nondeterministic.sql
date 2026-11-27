-- https://github.com/ClickHouse/ClickHouse/issues/116939
-- `query_plan_merge_filter_into_join_condition` must not move a non-deterministic equality conjunct
-- into the join condition: there it is evaluated once per join input row instead of once per output
-- row, and the build-side runtime filter clones it into a second evaluation site. With a two-row
-- build side the merged plan drew `rand()` twice per query, so the count snapped to all-or-nothing.

SET enable_analyzer = 1;

SELECT count() BETWEEN 730000 AND 770000
FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
WHERE t1.a = t2.b + rand() % 2
SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT count() BETWEEN 730000 AND 770000
FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
WHERE t1.a = t2.b + rand() % 2
SETTINGS query_plan_merge_filter_into_join_condition = 0;

-- The conjunct stays in the filter.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + rand() % 2
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%rand%';

-- The same, when the non-deterministic call hides in the body of a lambda: it is invisible in the
-- outer `ActionsDAG`, because the lambda body is a DAG of its own.
SELECT 'lambda';
SELECT count() BETWEEN 730000 AND 770000
FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
WHERE t1.a = t2.b + arrayExists(x -> (rand(x) % 2) = 0, materialize([1]))
SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + arrayExists(x -> (rand(x) % 2) = 0, materialize([1]))
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%arrayExists%';

-- The `WITH` form of the same shape: a lambda alias is inlined by the analyzer, so it builds the
-- very same `ActionsDAG`.
SELECT count() FROM (
    EXPLAIN actions = 1
    WITH y -> (rand(y) % 2) = 0 AS f
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + arrayExists(x -> f(x), materialize([1]))
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%arrayExists%';

-- The same, when the lambda holding the non-deterministic call is nested inside another lambda. The
-- planner hoists a lambda that captures nothing to the outermost level, so the enclosing lambda
-- *captures* it; the enclosing lambda then captures a constant only, is folded into a constant in
-- turn, and the nested lambda is left reachable only through `ColumnFunction::getCapturedColumns`.
SELECT 'nested lambda';
SELECT count() BETWEEN 730000 AND 770000
FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
WHERE t1.a = t2.b + arrayExists(x -> arrayExists(y -> (rand(y) % 2) = 0, [x]), materialize([1]))
SETTINGS query_plan_merge_filter_into_join_condition = 1;

SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + arrayExists(x -> arrayExists(y -> (rand(y) % 2) = 0, [x]), materialize([1]))
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%arrayExists%';

-- A deterministic equality is still merged.
SELECT 'deterministic';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + 1
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%';

-- A deterministic lambda does not block the merge.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + arrayExists(x -> (x % 2) = 0, materialize([1]))
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%';

-- Neither does a deterministic nested lambda.
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + arrayExists(x -> arrayExists(y -> (y % 2) = 0, [x]), materialize([1]))
    SETTINGS query_plan_merge_filter_into_join_condition = 1
) WHERE explain LIKE '%Join conditions:%';
