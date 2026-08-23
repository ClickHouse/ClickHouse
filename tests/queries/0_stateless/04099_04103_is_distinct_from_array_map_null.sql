SET enable_analyzer = 1;

-- Test: Array/Map IS DISTINCT FROM NULL returns wrong result (0 instead of 1)
-- Covers: FunctionsNullSafeCmp.h:150 hardcodes UInt8(0) for Array/Map vs NULL
-- regardless of is_equal_mode, breaking isDistinctFrom

-- Array IS DISTINCT FROM NULL should return 1 (they ARE distinct)
SELECT [1, 2, 3] IS DISTINCT FROM NULL;
SELECT [] IS DISTINCT FROM NULL;
SELECT NULL IS DISTINCT FROM [1, 2, 3];
SELECT NULL IS DISTINCT FROM [];

-- Map IS DISTINCT FROM NULL should return 1 (they ARE distinct)
SELECT map('a', 1) IS DISTINCT FROM NULL;
SELECT map() IS DISTINCT FROM NULL;
SELECT NULL IS DISTINCT FROM map('a', 1);
SELECT NULL IS DISTINCT FROM map();

-- Verify isNotDistinctFrom (<=>) still returns 0 correctly
SELECT [1, 2, 3] <=> NULL;
SELECT [] <=> NULL;
SELECT map('a', 1) <=> NULL;
SELECT map() <=> NULL;

-- Also test with table columns
DROP TABLE IF EXISTS t_04103;
CREATE TABLE t_04103 (a Array(Int32), m Map(String, Int32)) ENGINE = Memory;
INSERT INTO t_04103 VALUES ([1,2,3], map('k',1));
INSERT INTO t_04103 VALUES ([], map());

SELECT a IS DISTINCT FROM NULL, m IS DISTINCT FROM NULL FROM t_04103 ORDER BY length(a) DESC;
SELECT a <=> NULL, m <=> NULL FROM t_04103 ORDER BY length(a) DESC;
DROP TABLE IF EXISTS t_04103;
