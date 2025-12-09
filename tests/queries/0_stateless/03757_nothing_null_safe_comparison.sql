-- Fixes bug: https://github.com/ClickHouse/ClickHouse/issues/91834

SELECT 1 WHERE (assumeNotNull(materialize(NULL)), 1) <=> (1, 1);

-- Nothing <=> non-Nothing should return 0 (false)
SELECT 1 WHERE assumeNotNull(materialize(NULL)) <=> 1;

SELECT 1 WHERE (1, 1) <=> (1, 1);

SELECT 1 WHERE (NULL, 1) <=> (NULL, 1);
SELECT 1 WHERE NULL <=> NULL;

-- Test the return value directly using numbers(0) to avoid materializing Nothing columns
SELECT (assumeNotNull(materialize(NULL)), 1) <=> (1, 1) FROM numbers(0);
SELECT assumeNotNull(materialize(NULL)) <=> 1 FROM numbers(0);
SELECT (assumeNotNull(materialize(NULL)), 1) IS DISTINCT FROM (1, 1) FROM numbers(0);
SELECT assumeNotNull(materialize(NULL)) IS DISTINCT FROM 1 FROM numbers(0);

-- Type check
SELECT toTypeName(assumeNotNull(materialize(NULL))) FROM numbers(0);
