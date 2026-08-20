-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/96619
-- When JIT-compiling expressions that convert DateTime to DateTime64,
-- the scale multiplier was not applied, causing the value to be
-- reinterpreted instead of converted (e.g., seconds treated as milliseconds).
-- The Time/Time64 sections at the end cover the same scale lift for the other
-- date-time family that shares this code path.

SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT '--- DateTime to DateTime64 via multiIf ---';

SELECT multiIf(
    number = 0, toNullable(toDateTime('2001-02-03 04:05:06')),
    number = 1, toNullable(toDateTime('2001-02-03 04:05:06')),
    '1970-01-01 00:00:00'::Nullable(DateTime64(3))
) FROM numbers(2);

SELECT '--- DateTime to DateTime64 via if ---';

SELECT if(
    number % 2 = 0,
    toNullable(toDateTime('2001-02-03 04:05:06')),
    '1970-01-01 00:00:00'::Nullable(DateTime64(3))
) FROM numbers(2);

SELECT '--- CASE expression ---';

DROP TABLE IF EXISTS t;
CREATE TABLE t (date1 Nullable(DateTime), date2 Nullable(DateTime)) ENGINE = Memory;
INSERT INTO t(date1, date2) VALUES ('2001-02-03 04:05:06', NULL);

SELECT
    CASE
        WHEN date2 IS NOT NULL THEN date2
        WHEN date1 IS NOT NULL THEN date1
        ELSE '1970-01-01 00:00:00'::Nullable(DateTime64(3))
    END
FROM t;

DROP TABLE t;

-- Time and Time64 are native types, and the if/multiIf compilability check only
-- refuses to compile a mix of Date with DateTime. A Time/Time64 mix trips neither
-- predicate, so it stays on the compiled path and reaches the same scale multiplier.

SELECT '--- Time to Time64 via if ---';

SELECT if(number % 2 = 0, toTime('01:00:00'), toTime64('12:00:00.250', 3)) FROM numbers(2);

SELECT '--- Time64 scale lift via multiIf ---';

SELECT multiIf(
    number = 0, toTime64('01:00:00', 0),
    number = 1, toTime64('02:00:00.5', 1),
    toTime64('12:00:00.250', 3)
) FROM numbers(3);

SELECT '--- Time-family if is compiled, not interpreted ---';

-- The condition is a comparison of two stored columns: it is compilable but has no
-- compilable child, and a node with no compilable children is never compiled on its own.
-- So the outer if is the only node that can raise CompiledFunctionExecute in both queries.
DROP TABLE IF EXISTS t_cond;
CREATE TABLE t_cond (c1 UInt8, c2 UInt8, n1 UInt32, n2 UInt32) ENGINE = Memory;
INSERT INTO t_cond VALUES (0, 0, 1, 2), (1, 0, 3, 4);

SELECT if(c1 = c2, toTime('01:00:00'), toTime64('12:00:00.250', 3)) FROM t_cond
    SETTINGS log_comment = '03916_time_shape' FORMAT Null;

-- Control: compiles in any build that has the embedded compiler, so the comparison below
-- pins that the Time shape is compiled wherever anything is, without a no-msan tag.
SELECT if(c1 = c2, n1, n2) FROM t_cond
    SETTINGS log_comment = '03916_control_shape' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    (SELECT ProfileEvents['CompiledFunctionExecute'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '03916_time_shape' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1)
    = (SELECT ProfileEvents['CompiledFunctionExecute'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '03916_control_shape' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1);

DROP TABLE t_cond;
