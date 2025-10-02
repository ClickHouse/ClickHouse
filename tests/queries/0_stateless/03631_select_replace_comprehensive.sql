-- Test SELECT * REPLACE with all SQL clauses using new analyzer
-- This test verifies comprehensive fix for SELECT * REPLACE in all applicable clauses
-- See issue: https://github.com/ClickHouse/ClickHouse/issues/85313
-- See PR: https://github.com/ClickHouse/ClickHouse/pull/87630

SET allow_experimental_analyzer = 1;

-- Setup test tables
DROP TABLE IF EXISTS test_replace_main;
DROP TABLE IF EXISTS test_replace_merge;

CREATE TABLE test_replace_main (id UInt32, a UInt32, b UInt32, c String) ENGINE = Memory;
INSERT INTO test_replace_main VALUES (1, 100, 200, 'alpha'), (2, 300, 100, 'beta'), (3, 200, 300, 'gamma');

CREATE TABLE test_replace_merge (id UInt32, a UInt32, b UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_replace_merge VALUES (1, 100, 200), (2, 300, 100), (3, 200, 300);

-- Test 1: WHERE clause (original bug) - should use replaced values
SELECT '=== WHERE clause basic test ===';
SELECT * REPLACE(a AS b) FROM test_replace_main WHERE b > 150 ORDER BY id;

-- Test 1a: WHERE clause with arithmetic expressions (from original test)
SELECT '=== WHERE arithmetic expressions test ===';
DROP TABLE IF EXISTS test_arith;
CREATE TABLE test_arith (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO test_arith VALUES (1, 2), (2, 4);
SELECT * REPLACE(a + 10 AS b) FROM test_arith WHERE b = 11;
SELECT * REPLACE(a + 10 AS b) FROM test_arith WHERE b = 12;
SELECT * REPLACE(a * 10 + a AS b) FROM test_arith WHERE b = 11;

-- Test 1b: String replacement in WHERE (from original test)
DROP TABLE IF EXISTS test_replace_str;
CREATE TABLE test_replace_str (id UInt32, s String) ENGINE = Memory;
INSERT INTO test_replace_str VALUES (1, 'hello'), (2, 'world');
SELECT * REPLACE(s || '_suffix' AS s) FROM test_replace_str WHERE s = 'hello_suffix';

DROP TABLE test_arith;
DROP TABLE test_replace_str;

-- Test 2: ORDER BY clause - should sort by replaced values
SELECT '=== ORDER BY clause test ===';
SELECT * REPLACE(a AS b) FROM test_replace_main ORDER BY b;

-- Test 3: PREWHERE clause (MergeTree only) - should use replaced values
SELECT '=== PREWHERE clause test ===';
SELECT * REPLACE(a AS b) FROM test_replace_merge PREWHERE b > 150 ORDER BY id;

-- Test 4: LIMIT BY clause - should group by replaced values
SELECT '=== LIMIT BY clause test ===';
DROP TABLE IF EXISTS test_limit_by;
CREATE TABLE test_limit_by (id UInt32, a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO test_limit_by VALUES (1, 100, 200), (2, 100, 300), (3, 200, 400), (4, 200, 500);
SELECT * REPLACE(a AS b) FROM test_limit_by ORDER BY id LIMIT 1 BY b < 200;
DROP TABLE test_limit_by;

-- Test 5: WINDOW clause - should order by replaced values in window
SELECT '=== WINDOW clause test ===';
SELECT * REPLACE(a AS b), ROW_NUMBER() OVER (ORDER BY b) as rn
FROM test_replace_main
ORDER BY id;

-- Test 6: Complex expressions in WHERE
SELECT '=== Complex expressions test ===';
SELECT * REPLACE(a * 2 + 10 AS b) FROM test_replace_main WHERE b > 250 ORDER BY id;

-- Test 7: Multiple REPLACE columns
SELECT '=== Multiple REPLACE test ===';
SELECT * REPLACE(a AS b, 'replaced_' || c AS c)
FROM test_replace_main
WHERE b > 150
ORDER BY id;

-- Test 8: Subquery with REPLACE
SELECT '=== Subquery test ===';
SELECT * FROM (
    SELECT * REPLACE(a AS b) FROM test_replace_main WHERE b > 150
) ORDER BY id;

-- Test 9: String operations in WHERE
SELECT '=== String operations test ===';
SELECT * REPLACE(c || '_suffix' AS c)
FROM test_replace_main
WHERE c = 'beta_suffix'
ORDER BY id;

-- Test 10: Multiple clauses together
SELECT '=== Multiple clauses test ===';
SELECT * REPLACE(a AS b) FROM test_replace_main
WHERE b > 150
ORDER BY b
LIMIT 2;

-- Test 11: QUALIFY clause with replaced values
SELECT '=== QUALIFY clause test ===';
SELECT * REPLACE(a AS b), ROW_NUMBER() OVER (ORDER BY b) as rn
FROM test_replace_main
QUALIFY rn <= 2
ORDER BY id;

-- Test 12: Nested expressions in clauses
SELECT '=== Nested expressions test ===';
SELECT * REPLACE(a + 100 AS b) FROM test_replace_main WHERE b + 50 > 200 ORDER BY b;

-- Test 13: String comparison with replaced column
SELECT '=== String WHERE test ===';
SELECT * REPLACE('modified_' || c AS c) FROM test_replace_main WHERE c = 'modified_beta' ORDER BY id;

-- Test 14: Edge case - replacement with same column name
SELECT '=== Same column replacement test ===';
SELECT * REPLACE(a + 1000 AS a) FROM test_replace_main WHERE a > 1100 ORDER BY id;

-- Test 15: Complex ORDER BY with replaced column
SELECT '=== Complex ORDER BY test ===';
SELECT * REPLACE(a AS b) FROM test_replace_main ORDER BY b DESC, id;

-- Test 16: GROUP BY clause with replaced values in subquery
SELECT '=== GROUP BY subquery test ===';
DROP TABLE IF EXISTS test_group_by;
CREATE TABLE test_group_by (id UInt32, category String, value UInt32) ENGINE = Memory;
INSERT INTO test_group_by VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40);
SELECT category, sum(value) as total
FROM (SELECT * REPLACE(value * 10 AS value) FROM test_group_by)
GROUP BY category
ORDER BY category;
DROP TABLE test_group_by;

-- Test 17: GROUP BY clause with replaced column directly
SELECT '=== GROUP BY direct test ===';
DROP TABLE IF EXISTS test_group_by_direct;
CREATE TABLE test_group_by_direct (id UInt32, category String, value UInt32) ENGINE = Memory;
INSERT INTO test_group_by_direct VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 40);
SELECT * REPLACE(category || '_modified' AS category), sum(value) as total
FROM test_group_by_direct
GROUP BY category
ORDER BY category;
DROP TABLE test_group_by_direct;

-- Test 18: HAVING clause with replaced values in subquery
SELECT '=== HAVING subquery test ===';
DROP TABLE IF EXISTS test_having;
CREATE TABLE test_having (id UInt32, category String, amount UInt32) ENGINE = Memory;
INSERT INTO test_having VALUES (1, 'X', 50), (2, 'X', 75), (3, 'Y', 100), (4, 'Y', 125);
SELECT category, sum(amount) as total
FROM (SELECT * REPLACE(amount + 100 AS amount) FROM test_having)
GROUP BY category
HAVING total > 200
ORDER BY category;
DROP TABLE test_having;

-- Test 19: HAVING clause with replaced column directly
SELECT '=== HAVING direct test ===';
DROP TABLE IF EXISTS test_having_direct;
CREATE TABLE test_having_direct (id UInt32, category String, amount UInt32) ENGINE = Memory;
INSERT INTO test_having_direct VALUES (1, 'X', 50), (2, 'X', 75), (3, 'Y', 100), (4, 'Y', 125);
SELECT * REPLACE(amount + 100 AS amount), category, sum(amount) as total
FROM test_having_direct
GROUP BY category
HAVING total > 200
ORDER BY category;
DROP TABLE test_having_direct;

-- Cleanup
DROP TABLE test_replace_main;
DROP TABLE test_replace_merge;