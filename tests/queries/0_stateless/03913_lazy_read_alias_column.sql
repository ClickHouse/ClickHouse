-- Regression test for issue #96452: LazilyReadFromMergeTree optimization
-- was disabled when selecting ALIAS columns (alias expansion forced full read).
-- Selecting only an ALIAS column with ORDER BY + LIMIT should use lazy read
-- like the source column.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes_for_top_k = 1;

DROP TABLE IF EXISTS test_lazy_alias SYNC;
CREATE TABLE test_lazy_alias
(
    time       DateTime64(3),
    body       String,
    body_alias String ALIAS if(length(body) > 5, 'long', 'short'),
    severity   LowCardinality(String),
    INDEX time_minmax(time) TYPE minmax GRANULARITY 1,
    INDEX severity_set(severity) TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test_lazy_alias
SELECT
    toDateTime64('2020-01-01 00:00:00', 3) - number AS time,
    repeat('x', number % 20) AS body,
    if(number % 2 == 0, 'info', 'medium') AS severity
FROM numbers(10000);

-- Selecting ALIAS column with ORDER BY + LIMIT: result must match expression on source column
SELECT body_alias
FROM test_lazy_alias
WHERE severity = 'medium'
ORDER BY time DESC
LIMIT 10;

-- Same result when using the expression explicitly (sanity check)
SELECT if(length(body) > 5, 'long', 'short') AS body_alias
FROM test_lazy_alias
WHERE severity = 'medium'
ORDER BY time DESC
LIMIT 10;

DROP TABLE IF EXISTS test_lazy_alias SYNC;
