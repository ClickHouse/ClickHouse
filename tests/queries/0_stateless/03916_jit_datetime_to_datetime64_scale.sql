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

SELECT '--- Time-family if/multiIf is compiled, not interpreted ---';

-- Every condition is a comparison of two stored columns: compilable, but with no compilable
-- child, and a node with no compilable children is never compiled on its own. The branches
-- are constants or plain columns, so the outer if/multiIf is the only node that can raise
-- CompiledFunctionExecute in any of the four queries below.
DROP TABLE IF EXISTS t_cond;
CREATE TABLE t_cond (c1 UInt8, c2 UInt8, n1 UInt32, n2 UInt32) ENGINE = Memory;
INSERT INTO t_cond VALUES (0, 0, 1, 2), (1, 0, 3, 4);

SELECT if(c1 = c2, toTime('01:00:00'), toTime64('12:00:00.250', 3)) FROM t_cond
    SETTINGS log_comment = '03916_time_if_shape' FORMAT Null;

SELECT multiIf(c1 = c2, toTime('01:00:00'), c1 > c2, toTime64('02:00:00.5', 1), toTime64('12:00:00.250', 3)) FROM t_cond
    SETTINGS log_comment = '03916_time_multiif_shape' FORMAT Null;

-- Controls: the same shapes over UInt32 compile in any build that has the embedded compiler,
-- so the comparisons below pin that the Time shapes are compiled wherever anything is,
-- without a no-msan tag.
SELECT if(c1 = c2, n1, n2) FROM t_cond
    SETTINGS log_comment = '03916_control_if_shape' FORMAT Null;

SELECT multiIf(c1 = c2, n1, c1 > c2, n2, n1) FROM t_cond
    SETTINGS log_comment = '03916_control_multiif_shape' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '03916_%_shape'
    GROUP BY log_comment
)
-- The third column keeps the multiIf pair honest: 04930_jit_compiled_if_date_to_datetime anchors
-- if to CompiledFunctionExecute > 0, so a regression that stops compiling only multiIf leaves the
-- if control at 1 and the multiIf control at 0 instead of collapsing both pairs to a green 0 = 0.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '03916_time_if_shape')
        = (SELECT compiled FROM shapes WHERE log_comment = '03916_control_if_shape'),
    (SELECT compiled FROM shapes WHERE log_comment = '03916_time_multiif_shape')
        = (SELECT compiled FROM shapes WHERE log_comment = '03916_control_multiif_shape'),
    (SELECT compiled FROM shapes WHERE log_comment = '03916_control_multiif_shape')
        = (SELECT compiled FROM shapes WHERE log_comment = '03916_control_if_shape');

DROP TABLE t_cond;
