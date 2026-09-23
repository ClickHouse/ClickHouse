-- The fold must give the runtime answer: a full String compares with an Enum by name, a const one by code (#121723)
SELECT countIf(materialize('a') < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)')) FROM numbers(2);
SELECT count() FROM numbers(2) WHERE materialize('a') < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)');
SELECT count() FROM numbers(2) WHERE NOT (materialize('a') < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'));
SELECT count() FROM numbers(2) WHERE materialize(tuple('a', 1)) < tuple(CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'), 1);
SELECT count() FROM (SELECT 'a' AS s FROM numbers(2) UNION ALL SELECT 'z' AS s FROM numbers(2))
WHERE s < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)');

SELECT 'folded', countIf(explain LIKE '%Filter column: 1%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(2) WHERE materialize('a') < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)'));

-- The fold is a plan optimization, so `query_plan_enable_optimizations = 0` disables it
SET query_plan_enable_optimizations = 0;
SELECT 'not folded', countIf(explain LIKE '%Filter column: 1%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(2) WHERE materialize('online') = 'online');
SELECT count() FROM numbers(2) WHERE materialize('a') < CAST('z', 'Enum8(''z'' = 1, ''a'' = 2)');
