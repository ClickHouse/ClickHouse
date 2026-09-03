-- An attempt to reproduce https://github.com/ClickHouse/ClickHouse/issues/70015
-- The issue was that binary arithmetic operations like minus would throw
-- "Arguments of 'minus' have incorrect data types" logical error when
-- the arguments had different numeric types (e.g., Int64 - Int32).
-- However, this test didn't reproduce the issue in any previous ClickHouse versions,
-- and the actual issue was already fixed a long time ago.

SET compile_expressions = 0; -- Disable JIT to avoid unrelated issues

DROP TABLE IF EXISTS t_70015;
CREATE TABLE t_70015 (
    c_izfnu Int64,
    c_l8d2_b Int64,
    c_hmbcdw Int64,
    c_zjw Int32
) ENGINE = Memory;

INSERT INTO t_70015 VALUES (1, 1, 2, 3), (2, 3, 4, 5);

-- The problematic query pattern from the issue:
-- if(CAST(equals(), 'Nullable(Bool)'), sign(Int64), Int64) - Int32
SELECT if(CAST(c_izfnu = c_l8d2_b, 'Nullable(Bool)'), sign(c_izfnu), c_hmbcdw) - c_zjw AS result
FROM t_70015
ORDER BY result;

-- Additional test cases for type promotion in minus
SELECT toInt64(10) - toInt32(5);
SELECT toInt64(10) - toInt16(5);
SELECT toInt64(10) - toInt8(5);
SELECT toInt32(10) - toInt64(5);

DROP TABLE t_70015;
