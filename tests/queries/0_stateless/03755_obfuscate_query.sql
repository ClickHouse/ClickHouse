-- Ensure obfuscateQuery produces different results for different rows even for constant query
SELECT uniq(obfuscateQuery('select ' || toString(number))) FROM numbers(2);

-- Ensure obfuscateQueryWithSeed is deterministic for the same seed
SELECT uniq(obfuscateQueryWithSeed('select 1', 42)) AS uniq_seed FROM numbers(2);

-- Test obfuscateQueryWithSeed with string seed
SELECT uniq(obfuscateQueryWithSeed('select name from users', 'myseed')) AS uniq_string_seed FROM numbers(2);

-- Test that obfuscateQuery preserves query structure
SELECT match(obfuscateQuery('SELECT name, age FROM users WHERE age > 30'), 'SELECT.*FROM.*WHERE') AS structure_preserved;

-- Test obfuscateQuery with complex query
SELECT length(obfuscateQuery('SELECT a, b, c FROM table WHERE x = 1 AND y = 2 GROUP BY a ORDER BY b LIMIT 10')) > 0 AS non_empty_result;

-- Test obfuscateQueryWithSeed produces same result with same seed
SELECT
    obfuscateQueryWithSeed('SELECT * FROM t', 100) = obfuscateQueryWithSeed('SELECT * FROM t', 100) AS deterministic1,
    obfuscateQueryWithSeed('SELECT * FROM t', 100) = obfuscateQueryWithSeed('SELECT * FROM t', 200) AS deterministic2;

-- Test tag behavior: same tag should produce same obfuscated result, different tag should differ
SELECT
    obfuscateQuery('SELECT user_id, amount FROM orders', 'tag_A') = obfuscateQuery('SELECT user_id, amount FROM orders', 'tag_A') AS same_tag_equal,
    obfuscateQuery('SELECT user_id, amount FROM orders', 'tag_A') != obfuscateQuery('SELECT user_id, amount FROM orders', 'tag_B') AS different_tag_diff;

-- Test obfuscateQuery with column input
SELECT uniq(obfuscateQuery(query_string)) AS uniq_column_input
FROM (SELECT 'SELECT ' || toString(number) || ' FROM t' AS query_string FROM numbers(2));

