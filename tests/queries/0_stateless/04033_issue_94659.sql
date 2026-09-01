-- Issue: https://github.com/ClickHouse/ClickHouse/issues/94659
-- ROW POLICY + ALIAS column with dictGet should not cause LOGICAL_ERROR

SET enable_analyzer = 1;
SET optimize_respect_aliases = 1;

DROP ROW POLICY IF EXISTS pol_94659 ON t1_94659;
DROP TABLE IF EXISTS t1_94659;
DROP DICTIONARY IF EXISTS d1_94659;
DROP TABLE IF EXISTS t2_94659;

-- Setup dictionary source table
CREATE TABLE t2_94659 (a Int64, b Int64) ENGINE = Memory();
INSERT INTO t2_94659 VALUES (11, 21);

-- Setup dictionary
CREATE DICTIONARY d1_94659 (a Int64, b Int64)
PRIMARY KEY a
LIFETIME(MIN 0 MAX 0)
SOURCE(CLICKHOUSE(TABLE 't2_94659'))
LAYOUT(COMPLEX_KEY_HASHED());

-- Setup table with dictGet ALIAS column
CREATE TABLE t1_94659 (
  a Int64,
  b Int64 ALIAS dictGet(d1_94659, 'b', a),
  c Int64
) ENGINE = Memory();

INSERT INTO t1_94659 (a, c) VALUES (11, 15);

-- Test 1: Without ROW POLICY (baseline)
SELECT *, b FROM t1_94659;

-- Test 2: Add ROW POLICY and verify query works
CREATE ROW POLICY pol_94659 ON t1_94659 USING c IS NOT NULL TO ALL;
SELECT *, b FROM t1_94659;

-- Test 3: Query not referencing ALIAS column (also triggered the bug)
SELECT a, c FROM t1_94659;

-- Test 4: Only select ALIAS column
SELECT b FROM t1_94659;

-- Test 5: Verify with old analyzer still works
SELECT *, b FROM t1_94659 SETTINGS enable_analyzer = 0;

DROP ROW POLICY pol_94659 ON t1_94659;

-- Cleanup
DROP TABLE t1_94659;
DROP DICTIONARY d1_94659;
DROP TABLE t2_94659;
