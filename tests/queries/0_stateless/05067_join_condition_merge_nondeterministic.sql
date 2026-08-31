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
) WHERE explain LIKE '%Join conditions:%rand%';

-- A deterministic equality is still merged.
SELECT 'deterministic';
SELECT count() FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT number % 2 AS a FROM numbers(1000)) t1
    CROSS JOIN (SELECT DISTINCT number AS b FROM numbers(2)) t2
    WHERE t1.a = t2.b + 1
) WHERE explain LIKE '%Join conditions:%';
