-- Regression test for issue #96452: LazilyReadFromMergeTree optimization
-- was disabled when selecting ALIAS columns with ORDER BY … LIMIT.

SET enable_analyzer = 1, query_plan_optimize_lazy_materialization = true, query_plan_max_limit_for_lazy_materialization = 10;

DROP TABLE IF EXISTS test_lazy_alias SYNC;
CREATE TABLE test_lazy_alias
(
    time       DateTime64(3),
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short'),
    severity   LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 100;

INSERT INTO test_lazy_alias
SELECT
    toDateTime64('2020-01-01 00:00:00', 3) - number AS time,
    repeat('x', number % 20) AS body,
    if(number % 2 == 0, 'info', 'medium') AS severity
FROM numbers(10000);

-- 1. Baseline: LazilyReadFromMergeTree appears when selecting a physical column.
SELECT 'physical_column_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN SELECT body FROM test_lazy_alias ORDER BY time DESC LIMIT 10)
WHERE s LIKE 'LazilyRead%';

-- 2. Same optimization must also apply when selecting an ALIAS column.
SELECT 'alias_column_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN SELECT body_alias FROM test_lazy_alias ORDER BY time DESC LIMIT 10)
WHERE s LIKE 'LazilyRead%';

-- 3. The reported regression is the filtered top-K shape `WHERE ... ORDER BY ... LIMIT`.
--    Assert the optimization is applied there too, not only for the unfiltered query,
--    so result correctness alone cannot hide a lost optimization.
SELECT 'alias_filtered_plan';
SELECT trimLeft(explain) AS s
FROM (EXPLAIN SELECT body_alias FROM test_lazy_alias WHERE severity = 'medium' ORDER BY time DESC LIMIT 10)
WHERE s LIKE 'LazilyRead%';

-- 4. Verify correctness: ALIAS column result must match the expression on source column.
SELECT 'alias_result';
SELECT body_alias
FROM test_lazy_alias
WHERE severity = 'medium'
ORDER BY time DESC
LIMIT 10;

SELECT 'explicit_result';
SELECT if(length(body) > 5, 'long', 'short') AS body_alias
FROM test_lazy_alias
WHERE severity = 'medium'
ORDER BY time DESC
LIMIT 10;

DROP TABLE IF EXISTS test_lazy_alias SYNC;
