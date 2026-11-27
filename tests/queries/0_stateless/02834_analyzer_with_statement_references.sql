SET enable_analyzer = 1;

WITH test_aliases AS (SELECT number FROM numbers(20)), alias2 AS (SELECT number FROM test_aliases)
SELECT number FROM alias2 SETTINGS enable_global_with_statement = 1;

WITH test_aliases AS (SELECT number FROM numbers(20)), alias2 AS (SELECT number FROM test_aliases)
SELECT number FROM alias2 SETTINGS enable_global_with_statement = 0; -- { serverError UNKNOWN_TABLE }
