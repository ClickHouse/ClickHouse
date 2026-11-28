-- The compiled bitShiftRight/bitShiftLeft emitted LLVM shifts without guarding the amount, and a shift by the bit
-- width or more is poison: `bitAnd(bitShiftRight(x, 63), 1) = 0` compiled to 0 for a UInt8 x while the interpreter
-- gives 1. The shift amount was also cast to the result type, truncating e.g. 256 to 0.
-- Found by json_ast_sql_execution_fuzzer (differential oracle, JIT variant).
SET compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT bitAnd(bitShiftRight(*, 63), 1) = 0 FROM system.one;
SELECT bitAnd(bitShiftRight(number, 63), 1) = 0, bitShiftRight(toUInt8(number), 8) + 0, bitShiftLeft(toUInt8(number + 1), 8) + 0 FROM numbers(3);
SELECT bitShiftRight(toUInt8(number + 200), toUInt16(256)) + 0, bitShiftLeft(toUInt8(number + 1), toUInt16(256)) + 0, bitShiftRight(toInt8(number - 100), 63) + 0 FROM numbers(2);
SELECT bitShiftRight(toUInt8(number + 200), 7) + 0, bitShiftLeft(toUInt16(number + 1), 15) + 0, bitShiftRight(toInt64(number - 100), 63) + 0 FROM numbers(2);
SET compile_expressions = 0;
SELECT bitAnd(bitShiftRight(*, 63), 1) = 0 FROM system.one;
SELECT bitAnd(bitShiftRight(number, 63), 1) = 0, bitShiftRight(toUInt8(number), 8) + 0, bitShiftLeft(toUInt8(number + 1), 8) + 0 FROM numbers(3);
SELECT bitShiftRight(toUInt8(number + 200), toUInt16(256)) + 0, bitShiftLeft(toUInt8(number + 1), toUInt16(256)) + 0, bitShiftRight(toInt8(number - 100), 63) + 0 FROM numbers(2);
SELECT bitShiftRight(toUInt8(number + 200), 7) + 0, bitShiftLeft(toUInt16(number + 1), 15) + 0, bitShiftRight(toInt64(number - 100), 63) + 0 FROM numbers(2);
