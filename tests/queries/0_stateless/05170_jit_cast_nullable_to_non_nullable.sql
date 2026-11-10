-- A NULL converted to a non-Nullable type raises instead of propagating, so such a conversion
-- must not be JIT-compiled: a compiled expression produces a value and has no way to raise.
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_cast_null;
DROP TABLE IF EXISTS t_jit_cast_no_null;
DROP TABLE IF EXISTS t_jit_cast_plain;

CREATE TABLE t_jit_cast_null (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_jit_cast_null VALUES (1), (NULL), (3);
CREATE TABLE t_jit_cast_no_null (x Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_jit_cast_no_null VALUES (1), (2), (3);
CREATE TABLE t_jit_cast_plain (y Int64) ENGINE = Memory;
INSERT INTO t_jit_cast_plain VALUES (1), (2), (3);

-- The destination cannot hold the NULL row, in a projection and in a filter, where returning a
-- value would silently change the row count instead.
SELECT CAST(x AS Int32) + 1 FROM t_jit_cast_null; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_jit_cast_null WHERE CAST(x AS Int32) + 1 > 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SET compile_expressions = 0;
SELECT CAST(x AS Int32) + 1 FROM t_jit_cast_null; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_jit_cast_null WHERE CAST(x AS Int32) + 1 > 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SET compile_expressions = 1, min_count_to_compile_expression = 0;

-- A Nullable source holding no NULL still converts, and a Nullable destination still carries the flag.
SELECT CAST(x AS Int32) + 1 FROM t_jit_cast_no_null ORDER BY x;
SELECT CAST(x AS Nullable(Int32)) + 1 FROM t_jit_cast_null ORDER BY x;

SET compile_expressions = 0;
SELECT CAST(x AS Int32) + 1 FROM t_jit_cast_no_null ORDER BY x;
SELECT CAST(x AS Nullable(Int32)) + 1 FROM t_jit_cast_null ORDER BY x;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

-- Every row above is a value oracle, so all of them would still pass if the conversion silently
-- stopped or started being compiled. The shapes below pin which of them compiles. Each conversion
-- shape holds exactly two compilable nodes, the addition and the conversion fused into it, so once
-- the conversion is declined nothing is left to compile. `CompiledFunctionExecute` counts executions
-- of an already-compiled node, so a warm compiled cache does not change any of them.
SELECT CAST(x AS Int32) + 1 FROM t_jit_cast_no_null
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05170_declined' FORMAT Null;
SELECT CAST(x AS Nullable(Int32)) + 1 FROM t_jit_cast_null
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05170_nullable_target' FORMAT Null;
SELECT CAST(y AS Int32) + 1 FROM t_jit_cast_plain
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05170_control' FORMAT Null;
SELECT (y + 1) * 2 FROM t_jit_cast_plain
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05170_arith' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05170_%'
    GROUP BY log_comment
)
-- `05170_arith` contains no conversion, so it measures whether the embedded compiler is working at
-- all, independently of `FunctionCast`. Comparing both surviving conversion shapes against it keeps
-- the row red when conversions stop being compiled and green when the compiler is simply absent.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05170_declined') = 0,
    (SELECT compiled FROM shapes WHERE log_comment = '05170_nullable_target')
        = (SELECT compiled FROM shapes WHERE log_comment = '05170_arith'),
    (SELECT compiled FROM shapes WHERE log_comment = '05170_control')
        = (SELECT compiled FROM shapes WHERE log_comment = '05170_arith');

DROP TABLE t_jit_cast_null;
DROP TABLE t_jit_cast_no_null;
DROP TABLE t_jit_cast_plain;
