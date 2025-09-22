-- Test SELECT * REPLACE with WHERE clause using new analyzer
-- This test verifies that WHERE clause uses replaced column values
-- See issue: https://github.com/ClickHouse/ClickHouse/issues/85313

DROP TABLE IF EXISTS test_replace;

CREATE TABLE test_replace (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO test_replace VALUES (1, 2), (2, 4);

-- Test 1: Basic REPLACE with WHERE clause should use replaced values
-- WHERE b = 11 should match row where replaced b=a+10=1+10=11
SELECT * REPLACE(a + 10 AS b) FROM test_replace WHERE b = 11;

-- Test 2: Different WHERE condition
-- WHERE b = 12 should match row where replaced b=a+10=2+10=12
SELECT * REPLACE(a + 10 AS b) FROM test_replace WHERE b = 12;

-- Test 3: Complex expression replacement
-- WHERE b = 11 should match row where replaced b=a*10+a=1*10+1=11
SELECT * REPLACE(a * 10 + a AS b) FROM test_replace WHERE b = 11;

-- Test 4: String replacement
DROP TABLE IF EXISTS test_replace_str;
CREATE TABLE test_replace_str (id UInt32, s String) ENGINE = Memory;
INSERT INTO test_replace_str VALUES (1, 'hello'), (2, 'world');

SELECT * REPLACE(s || '_suffix' AS s) FROM test_replace_str WHERE s = 'hello_suffix';

DROP TABLE test_replace;
DROP TABLE test_replace_str;