-- The compiled body of a binary function must use the signedness of the extended integer type: with
-- `Int128` treated as unsigned, `least`, `greatest`, `midpoint` and `bitShiftRight` returned the
-- wrong value for negative operands as soon as the expression was compiled.

DROP TABLE IF EXISTS t_jit_int128;

CREATE TABLE t_jit_int128 (a Int128, b Int128) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_jit_int128 VALUES (-5, 3), (-7, 2), (-8, 1);

SELECT 'compiled';
SELECT least(a + toInt128(0), b), greatest(a + toInt128(0), b), bitShiftRight(a + toInt128(0), 1), midpoint(a + toInt128(0), b)
FROM t_jit_int128 ORDER BY a
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT 'interpreted';
SELECT least(a + toInt128(0), b), greatest(a + toInt128(0), b), bitShiftRight(a + toInt128(0), 1), midpoint(a + toInt128(0), b)
FROM t_jit_int128 ORDER BY a
SETTINGS compile_expressions = 0;

DROP TABLE t_jit_int128;
