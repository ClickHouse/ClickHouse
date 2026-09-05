-- Compiled code cannot throw: an argument type whose `apply` raises is left uncompiled and raises
-- there, and the decline is by type, so a signed shift count is declined whole though only a
-- negative one raises. Both paths return zero for a count at or past the first argument's bit width.

DROP TABLE IF EXISTS t_jit_bit_shift;
CREATE TABLE t_jit_bit_shift (c0 UInt8) ENGINE = Memory;
INSERT INTO t_jit_bit_shift VALUES (7), (127), (230);

-- `bitNot` is the compilable child that makes the shape compilable at all: a lone shift over a
-- table column is never compiled. `materialize` keeps the count out of constant folding.

-- A big-integer shift count, and either big-integer operand of a rotate, are not implemented. A
-- big-integer first argument of a shift is implemented, and stays compiled: see the shapes below.
-- The shift counts here are unsigned, so they are declined for being big integers, not for signedness.
SELECT bitShiftLeft(bitNot(c0), materialize(toUInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitShiftRight(bitNot(c0), materialize(toUInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateLeft(bitNot(toInt128(c0)), materialize(toUInt8(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateLeft(bitNot(toUInt8(c0)), materialize(toInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateRight(bitNot(toInt128(c0)), materialize(toUInt8(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }
SELECT bitRotateRight(bitNot(c0), materialize(toInt128(2))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError NOT_IMPLEMENTED }

-- A negative shift count is out of bounds.
SELECT bitShiftLeft(bitNot(c0), materialize(toInt8(-1))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT bitShiftRight(bitNot(c0), materialize(toInt8(-1))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The interpreted path is the reference: a count exactly at the bit width of the first argument
-- yields zero, in the width of that argument and not in the wider width of the result.
SELECT bitShiftLeft(bitNot(toUInt8(127)), toUInt16(8));

-- The compiled and the interpreted path agree: at the width of the first argument with a wider
-- count type, at or past the width of the result type where the shift itself would be poison, and
-- at an in-range count.
SELECT (SELECT groupArray(bitShiftLeft(bitNot(toUInt8(c0)), materialize(toUInt16(8)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftLeft(bitNot(toUInt8(c0)), materialize(toUInt16(8)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);
SELECT (SELECT groupArray(bitShiftLeft(bitNot(toInt128(c0)), materialize(toUInt8(128)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftLeft(bitNot(toInt128(c0)), materialize(toUInt8(128)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);
SELECT (SELECT groupArray(bitShiftLeft(bitNot(c0), materialize(toUInt8(3)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftLeft(bitNot(c0), materialize(toUInt8(3)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);

-- The same three cases for `bitShiftRight`. Its first argument is signed here, because an unsigned
-- one is zero-extended into the wider result and then has no bits above the count to disagree on.
SELECT (SELECT groupArray(bitShiftRight(bitNot(toInt8(c0)), materialize(toUInt16(8)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftRight(bitNot(toInt8(c0)), materialize(toUInt16(8)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);
SELECT (SELECT groupArray(bitShiftRight(bitNot(toInt8(c0)), materialize(toUInt16(20)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftRight(bitNot(toInt8(c0)), materialize(toUInt16(20)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);
SELECT (SELECT groupArray(bitShiftRight(bitNot(toInt8(c0)), materialize(toUInt8(3)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitShiftRight(bitNot(toInt8(c0)), materialize(toUInt8(3)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);

-- A rotate takes its count modulo the bit width on both paths, so a negative count agrees there
-- while it is out of bounds for a shift. This is why only the shift gate refuses a signed count.
SELECT (SELECT groupArray(bitRotateLeft(bitNot(toUInt8(c0)), materialize(toInt8(-1)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(bitRotateLeft(bitNot(toUInt8(c0)), materialize(toInt8(-1)))) FROM t_jit_bit_shift
            SETTINGS compile_expressions = 0);

-- A native-width count still compiles for each of the four functions: unsigned for the shifts,
-- which decline a signed count by type, and signed for the rotates, which do not. A shift also
-- keeps compiling a big-integer first argument.
SELECT bitShiftLeft(bitNot(c0), materialize(toUInt8(3))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_shl' FORMAT Null;
SELECT bitShiftLeft(bitNot(toInt128(c0)), materialize(toUInt8(3))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_shl_big' FORMAT Null;
SELECT bitShiftRight(bitNot(c0), materialize(toUInt8(3))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_shr' FORMAT Null;
SELECT bitRotateLeft(bitNot(toUInt8(c0)), materialize(toInt8(3))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_rotl' FORMAT Null;
SELECT bitRotateRight(bitNot(toUInt8(c0)), materialize(toInt8(3))) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_rotr' FORMAT Null;

-- Control: an op compiled wherever anything is, in the same shape.
SELECT bitCount(bitNot(c0)) FROM t_jit_bit_shift
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05082_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Comparing the five shapes with the control instead of pinning a literal keeps this row green in a
-- build with no embedded compiler, where all six are 0.
WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05082_shl', '05082_shl_big', '05082_shr', '05082_rotl', '05082_rotr', '05082_control')
    GROUP BY log_comment
)
SELECT count() = 6 AND uniqExact(compiled) = 1 FROM shapes;

DROP TABLE t_jit_bit_shift;
