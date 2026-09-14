-- JIT-compiled code cannot throw, so `__bitSwapLastTwo` must reject the same argument types
-- when the expression is compiled as when it is interpreted.

DROP TABLE IF EXISTS t_bit_swap_last_two;
CREATE TABLE t_bit_swap_last_two (c0 UInt8) ENGINE = Memory;
INSERT INTO t_bit_swap_last_two VALUES (230), (250);

-- `__bitSwapLastTwo` accepts only `UInt8`. `toFloat64` is the compilable child that makes the
-- shape compilable at all: a lone `__bitSwapLastTwo(c0)` is never compiled.
SELECT __bitSwapLastTwo(toFloat64(c0)) FROM t_bit_swap_last_two
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError BAD_ARGUMENTS }
SELECT __bitSwapLastTwo(toFloat64(c0)) FROM t_bit_swap_last_two
    SETTINGS compile_expressions = 0; -- { serverError BAD_ARGUMENTS }

-- `UInt8` is the accepted argument type: the compiled and the interpreted path agree.
SELECT (SELECT groupArray(__bitSwapLastTwo(bitNot(c0))) FROM t_bit_swap_last_two
            SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0)
     = (SELECT groupArray(__bitSwapLastTwo(bitNot(c0))) FROM t_bit_swap_last_two
            SETTINGS compile_expressions = 0);

SELECT __bitSwapLastTwo(bitNot(c0)) FROM t_bit_swap_last_two
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05056_uint8' FORMAT Null;

-- Control: an op compiled wherever anything is, in the same shape.
SELECT bitCount(bitNot(c0)) FROM t_bit_swap_last_two
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0,
             log_comment = '05056_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- `UInt8` still compiles. The first column compares the two shapes rather than pinning a literal, so a
-- build without the embedded compiler stays green with both at 0; the second column requires that 0 to
-- coincide with the build actually lacking the compiler, so the pair cannot pass while JIT is present.
WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05056_uint8', '05056_control')
    GROUP BY log_comment
)
SELECT (SELECT compiled FROM shapes WHERE log_comment = '05056_uint8')
     = (SELECT compiled FROM shapes WHERE log_comment = '05056_control'),
       (SELECT compiled FROM shapes WHERE log_comment = '05056_control')
     = ((SELECT value FROM system.build_options WHERE name = 'USE_EMBEDDED_COMPILER') = 'ON');

DROP TABLE t_bit_swap_last_two;
