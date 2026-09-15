-- The compiled body of a function cannot throw, so it must not answer for the arguments the
-- interpreted implementation rejects: otherwise the same query raises an exception until the
-- expression gets compiled and then silently returns a value.

DROP TABLE IF EXISTS t_jit_bits;

CREATE TABLE t_jit_bits (c0 UInt8, s128 Int128, neg Int8) ENGINE = Memory;
INSERT INTO t_jit_bits VALUES (230, 2, -1), (250, 2, -2);

SELECT bitShiftLeft(bitNot(c0), s128) FROM t_jit_bits SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitShiftLeft(bitNot(c0), s128) FROM t_jit_bits SETTINGS compile_expressions = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitShiftRight(bitNot(c0), s128) FROM t_jit_bits SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateLeft(bitNot(c0), s128) FROM t_jit_bits SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateRight(bitNot(c0), s128) FROM t_jit_bits SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }

SELECT bitShiftLeft(bitNot(c0), neg) FROM t_jit_bits SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftLeft(bitNot(c0), neg) FROM t_jit_bits SETTINGS compile_expressions = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(bitNot(c0), neg) FROM t_jit_bits SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- An amount the width cannot hold answers zero, both interpreted and compiled.

SELECT bitShiftLeft(bitNot(c0), toUInt16(1000)), bitShiftRight(bitNot(c0), toUInt16(1000))
FROM t_jit_bits ORDER BY c0 SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT bitShiftLeft(bitNot(c0), toUInt16(1000)), bitShiftRight(bitNot(c0), toUInt16(1000))
FROM t_jit_bits ORDER BY c0 SETTINGS compile_expressions = 0;

-- The ordinary shapes are unchanged.

SELECT bitShiftLeft(bitNot(c0), toUInt8(2)), bitShiftRight(bitNot(c0), toUInt8(2)),
       bitRotateLeft(bitNot(c0), toUInt8(2)), bitRotateRight(bitNot(c0), toUInt8(2))
FROM t_jit_bits ORDER BY c0 SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT bitShiftLeft(bitNot(c0), toUInt8(2)), bitShiftRight(bitNot(c0), toUInt8(2)),
       bitRotateLeft(bitNot(c0), toUInt8(2)), bitRotateRight(bitNot(c0), toUInt8(2))
FROM t_jit_bits ORDER BY c0 SETTINGS compile_expressions = 0;

-- The compiled shift clamps at the width of the left operand, which the interpreted path uses too,
-- and not at the width both operands were widened to for the compiled body.

SELECT bitShiftLeft(bitNot(c0), toUInt16(8)), bitShiftRight(bitNot(c0), toUInt16(8))
FROM t_jit_bits ORDER BY c0 SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT bitShiftLeft(bitNot(c0), toUInt16(8)), bitShiftRight(bitNot(c0), toUInt16(8))
FROM t_jit_bits ORDER BY c0 SETTINGS compile_expressions = 0;

SELECT bitShiftRight(toInt8(-1) + toInt8(0), toUInt16(8)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT bitShiftRight(toInt8(-1) + toInt8(0), toUInt16(8)) SETTINGS compile_expressions = 0;

DROP TABLE t_jit_bits;
