-- JIT-compiled code cannot throw, so the compiled bit shifts and rotates must reject the same
-- argument types as the interpreted ones, and must apply the same rule for a shift count that
-- reaches the bit width of the first argument.

DROP TABLE IF EXISTS t_jit_bit_shift;
CREATE TABLE t_jit_bit_shift (c0 UInt8) ENGINE = Memory;
INSERT INTO t_jit_bit_shift VALUES (7), (127), (230);

-- `bitNot` is the compilable child that makes the shape compilable at all: a lone shift over a
-- table column is never compiled. `materialize` keeps the count out of constant folding.

-- A big-integer shift or rotate operand is not implemented.
SELECT bitShiftLeft(bitNot(c0), materialize(toInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitShiftRight(bitNot(c0), materialize(toInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateLeft(bitNot(toInt128(c0)), materialize(toUInt8(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateRight(bitNot(c0), materialize(toInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }

-- A negative shift count is out of bounds.
SELECT bitShiftLeft(bitNot(c0), materialize(toInt8(-1))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(bitNot(c0), materialize(toInt8(-1))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The interpreted path is the reference: a count at or past the bit width of the first argument
-- yields zero, in the width of that argument and not in the wider width of the result.
SELECT bitShiftLeft(bitNot(toUInt8(127)), toUInt16(9));

-- The compiled and the interpreted path agree: past the width of the first argument with a wider
-- count type, past the width of the result type, and at an in-range count.
SELECT (SELECT groupArray(bitShiftLeft(bitNot(toUInt8(c0)), materialize(toUInt16(9)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftLeft(bitNot(toUInt8(c0)), materialize(toUInt16(9)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);
SELECT (SELECT groupArray(bitShiftLeft(bitNot(toInt128(c0)), materialize(toUInt8(128)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftLeft(bitNot(toInt128(c0)), materialize(toUInt8(128)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);
SELECT (SELECT groupArray(bitShiftLeft(bitNot(c0), materialize(toUInt8(3)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftLeft(bitNot(c0), materialize(toUInt8(3)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);

-- An unsigned count of a native width still compiles.
SELECT bitShiftLeft(bitNot(c0), materialize(toUInt8(3))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_in_range' FORMAT Null;

-- Control: an op compiled wherever anything is, in the same shape.
SELECT bitCount(bitNot(c0)) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Comparing the two shapes instead of pinning a literal keeps this row green in a build with no
-- embedded compiler, where both are 0.
WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05082_in_range', '05082_control')
    GROUP BY log_comment
)
SELECT (SELECT compiled FROM shapes WHERE log_comment = '05082_in_range')
     = (SELECT compiled FROM shapes WHERE log_comment = '05082_control');

DROP TABLE t_jit_bit_shift;
