-- Test short identifier that starts with uppercase (regression test for buffer underread)
SELECT length(obfuscateQueryWithSeed('Ab', 1)) > 0;

-- Ensure obfuscateQuery produces different results per row for the same constant query string
SELECT uniq(obfuscateQuery('SELECT user_id, amount FROM orders WHERE amount > 10')) FROM numbers(2);

-- Ensure obfuscateQueryWithSeed is deterministic for the same seed
SELECT uniq(obfuscateQueryWithSeed('select 1', 42)) AS uniq_seed FROM numbers(2);
SELECT uniq(obfuscateQueryWithSeed('select 1', 42)) AS uniq_seed FROM numbers(2);
SELECT obfuscateQueryWithSeed('select 1'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Test obfuscateQueryWithSeed with string seed
SELECT uniq(obfuscateQueryWithSeed('select name from users', 'myseed')) AS uniq_string_seed FROM numbers(2);

-- Test that obfuscateQuery preserves query structure
SELECT match(obfuscateQuery('SELECT name, age FROM users WHERE age > 30'), 'SELECT.*FROM.*WHERE') AS structure_preserved;

-- Test obfuscateQuery with complex query in multiple queries
SELECT length(obfuscateQuery('SELECT a, b, c FROM table WHERE x = 1 AND y = 2 GROUP BY a ORDER BY b LIMIT 10')) > 0 AS non_empty_result;
SELECT length(obfuscateQuery('SELECT a, b, c FROM table WHERE x = 1 AND y = 2 GROUP BY a ORDER BY b LIMIT 10')) > 0 AS non_empty_result;

-- Test obfuscateQueryWithSeed in isolated subqueries to avoid common subexpression elimination
SELECT
    q1.obfuscated = q2.obfuscated AS deterministic1,
    q1.obfuscated = q3.obfuscated AS deterministic2
FROM (SELECT obfuscateQueryWithSeed('SELECT * FROM t', 100) AS obfuscated) AS q1
CROSS JOIN (SELECT obfuscateQueryWithSeed('SELECT * FROM t', 100) AS obfuscated) AS q2
CROSS JOIN (SELECT obfuscateQueryWithSeed('SELECT * FROM t', 200) AS obfuscated) AS q3;

-- Test tag argument types for obfuscateQuery
SELECT
    length(obfuscateQuery('SELECT user_id, amount FROM orders', 'tag_A')) > 0 AS string_tag_non_empty,
    length(obfuscateQuery('SELECT user_id, amount FROM orders', 42)) > 0 AS int_tag_non_empty;
SELECT obfuscateQuery('SELECT user_id, amount FROM orders', [1]); -- { serverError ILLEGAL_COLUMN }

-- Test obfuscateQuery with column input
SELECT uniq(obfuscateQuery(query_string)) AS uniq_column_input
FROM (SELECT 'SELECT ' || toString(number) || ' FROM t' AS query_string FROM numbers(2));

-- Test LowCardinality input for obfuscateQuery (should keep per-row randomness)
SELECT uniq(obfuscateQuery(materialize(CAST('SELECT user_id, amount FROM orders WHERE amount > 10', 'LowCardinality(String)')))) AS uniq_lc_query
FROM numbers(2);

-- Test LowCardinality input for obfuscateQueryWithSeed (should remain deterministic)
SELECT uniq(obfuscateQueryWithSeed(
    materialize(CAST('select 1', 'LowCardinality(String)')),
    materialize(CAST('seed', 'LowCardinality(String)')))) AS uniq_lc_seed
FROM numbers(2);

-- Test numeric literal overflow in obfuscation (readIntText overflows uint64 to zero for very large numbers)
SELECT length(obfuscateQueryWithSeed('SELECT 99999999999999999999999999999999 FROM t', 1)) > 0 AS overflow_handled;
