-- https://github.com/ClickHouse/ClickHouse/issues/117442
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

DROP TABLE IF EXISTS t_jit_float_cast;
CREATE TABLE t_jit_float_cast (c0 UInt8) ENGINE = Memory;
INSERT INTO t_jit_float_cast VALUES (230), (250);

-- In-range conversions are exact and identical compiled or interpreted.
SELECT c0,
       toFloat64(c0) / 10 <= CAST(toFloat64(c0) AS UInt8) AS in_range_by_cast,
       CAST(toFloat64(c0) AS Decimal32(2))                AS in_range_decimal
FROM t_jit_float_cast
ORDER BY c0;

-- A value the destination cannot hold raises. The non-finite one is built from the column so that
-- it is not constant folded before execution.
SELECT CAST(-toFloat64(c0) * 1e9 AS Decimal32(2)) FROM t_jit_float_cast; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(-toFloat64(c0) * 1e9, 2) FROM t_jit_float_cast; -- { serverError DECIMAL_OVERFLOW }
SELECT toUInt8(toFloat64(c0) / (toFloat64(c0) - toFloat64(c0))) FROM t_jit_float_cast; -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(toFloat64(c0) / (toFloat64(c0) - toFloat64(c0)) AS UInt8) FROM t_jit_float_cast; -- { serverError CANNOT_CONVERT_TYPE }

-- `Bool` is the one float to integer destination that stays compiled, because it is lowered as
-- `value != 0` rather than as a conversion, which is exact for every value.
SELECT CAST(-toFloat64(c0) * 1e30 AS Bool),
       CAST(toFloat64(c0) / (toFloat64(c0) - toFloat64(c0)) AS Bool)
FROM t_jit_float_cast
ORDER BY c0;

SET compile_expressions = 0;
SELECT CAST(-toFloat64(c0) * 1e30 AS Bool),
       CAST(toFloat64(c0) / (toFloat64(c0) - toFloat64(c0)) AS Bool)
FROM t_jit_float_cast
ORDER BY c0;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

-- The value an out-of-range float converts to is not defined by the language, so compare the compiled
-- and the interpreted evaluation of the same expression instead of pinning a literal.
CREATE TABLE t_jit_float_cast_arms (c0 UInt8, lte UInt8) ENGINE = Memory;

INSERT INTO t_jit_float_cast_arms
SELECT c0, toFloat64(c0) / 10 <= CAST(-toFloat64(c0) AS UInt8) FROM t_jit_float_cast;

SET compile_expressions = 0;
INSERT INTO t_jit_float_cast_arms
SELECT c0, toFloat64(c0) / 10 <= CAST(-toFloat64(c0) AS UInt8) FROM t_jit_float_cast;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT count() FROM (SELECT c0 FROM t_jit_float_cast_arms GROUP BY c0 HAVING uniqExact(lte) > 1);

DROP TABLE t_jit_float_cast_arms;

-- Every row above is a value oracle, so all of them would still pass if the conversions silently
-- stopped or started being compiled. The shapes below pin which of them compiles. Each has one
-- compilable child, so once its `CAST` is declined nothing is left to compile: a declined shape
-- reaches zero even where the control compiles. `CompiledFunctionExecute` counts executions of an
-- already-compiled node, so a warm compiled cache does not change any of them.
SELECT CAST(toFloat64(number) AS Bool) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05055_bool' FORMAT Null;
SELECT CAST(toFloat64(number) AS UInt8) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05055_declined' FORMAT Null;
SELECT toFloat64(number) + 1 FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05055_control' FORMAT Null;
-- The reported shape: a comparison whose right side is a declined conversion. The conversion becomes
-- an input to the compiled expression, so the comparison around it must still compile. Neither side
-- can compile on its own, so the counter here belongs to the comparison and to nothing else.
SELECT toFloat64(number) <= CAST(toFloat64(number) AS UInt8) FROM numbers(2)
    SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = '05055_parent' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05055_%'
    GROUP BY log_comment
)
-- The control keeps the first column honest in a build without the embedded compiler, where every
-- shape is interpreted and an absolute assertion would go green on nothing being compiled.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05055_bool')
        = (SELECT compiled FROM shapes WHERE log_comment = '05055_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05055_declined') = 0,
    (SELECT compiled FROM shapes WHERE log_comment = '05055_parent')
        = (SELECT compiled FROM shapes WHERE log_comment = '05055_control');

DROP TABLE t_jit_float_cast;
