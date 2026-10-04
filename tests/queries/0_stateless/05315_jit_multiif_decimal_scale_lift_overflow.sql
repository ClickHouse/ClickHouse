-- An `if`/`multiIf` that lifts a `Decimal`, `DateTime64` or `Time64` branch to a larger result scale gives the same result when
-- the lifted value does not fit the result's 32- or 64-bit storage, whether or not the expression is compiled: `DECIMAL_OVERFLOW`,
-- or, for a `DateTime64` / `Time64` branch of a result of the same type, the value clamped to the result's range.
SET compile_expressions = 1;
SET min_count_to_compile_expression = 0;

SELECT multiIf(number = 0, materialize(toDecimal32(999999999, 0)), number = 1, toDecimal32(1, 0), toDecimal32(1, 3)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toDecimal64(999999999999999999, 0)), number = 1, toDecimal64(1, 0), toDecimal64(1, 1)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
-- The declared precision does not bound the value: `Decimal(5, 0)` holds 999999999.
SELECT multiIf(number = 0, materialize(CAST(999999999 AS Decimal(5, 0))), number = 1, CAST(1 AS Decimal(5, 0)), toDecimal32(1, 4)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
-- A 32-bit branch lifted by 10^10 does not fit 64-bit storage.
SELECT multiIf(number = 0, materialize(toDecimal32(999999999, 0)), number = 1, toDecimal32(1, 0), toDecimal64(1, 10)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }
SELECT multiIf(number = 0, materialize(toNullable(toDecimal32(999999999, 0))), number = 1, toDecimal32(1, 0), toDecimal32(1, 3)) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }

DROP TABLE IF EXISTS t_jit_decimal_lift;
CREATE TABLE t_jit_decimal_lift (k UInt8, d Decimal32(0), e Decimal32(3)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_jit_decimal_lift VALUES (0, 999999999, 1.5), (1, 5, 2.5);
SELECT CASE WHEN k = 0 THEN d WHEN k = 2 THEN d ELSE e END FROM t_jit_decimal_lift ORDER BY k; -- { serverError DECIMAL_OVERFLOW }
DROP TABLE t_jit_decimal_lift;

-- A `DateTime64` inside its documented range overflows the lift to scale 9. The interpreted rescaling of a `DateTime64`
-- clamps it to the last representable tick, the default `date_time_overflow_behavior` of the implicit cast.
SELECT multiIf(number = 0, materialize(toDateTime64('2290-01-01 00:00:00', 0, 'UTC')), number = 1, toDateTime64('2000-01-01 00:00:00', 0, 'UTC'), toDateTime64('2000-01-01 00:00:00', 9, 'UTC')) FROM numbers(1);
SELECT if(number = 0, materialize(toDateTime64('2290-01-01 00:00:00', 0, 'UTC')), toDateTime64('2000-01-01 00:00:00', 9, 'UTC')) FROM numbers(1);
-- Interval arithmetic stores a `Time64` outside its display range; the rescaling of a `Time64` clamps it to the result's
-- clock window the same way.
SELECT multiIf(number = 0, materialize(addSeconds(toTime64('00:00:00', 0), 10000000000)), number = 1, toTime64('00:00:01', 0), toTime64('00:00:01', 9)) FROM numbers(1);
SELECT if(number = 0, materialize(addSeconds(toTime64('00:00:00', 0), 10000000000)), toTime64('00:00:01', 9)) FROM numbers(1);
SELECT multiIf(number = 0, materialize(addSeconds(toTime64('00:00:00', 0), 10000000000)), number = 1, toTime64('00:00:01', 0), toDateTime64('2000-01-01 00:00:00', 9, 'UTC')) FROM numbers(1); -- { serverError DECIMAL_OVERFLOW }

-- The widest lift that cannot overflow stays compiled and exact at the lowest value; one step wider is interpreted.
SELECT multiIf(number = 0, materialize(toDecimal32(-2147483648, 0)), number = 1, toDecimal32(1, 0), toDecimal64(1, 9)) FROM numbers(1);
SELECT multiIf(number = 0, materialize(toDecimal32(-2147483648, 0)), number = 1, toDecimal32(1, 0), toDecimal64(1, 9)) FROM numbers(2)
    SETTINGS log_comment = '05315_kept' FORMAT Null;
SELECT multiIf(number = 0, materialize(toDecimal32(1, 0)), number = 1, toDecimal32(1, 0), toDecimal64(1, 10)) FROM numbers(2)
    SETTINGS log_comment = '05315_declined' FORMAT Null;
SELECT materialize(2.0) + materialize(0.0) + materialize(1.0) FROM numbers(2)
    SETTINGS log_comment = '05315_control' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH shapes AS
(
    SELECT log_comment, argMax(ProfileEvents['CompiledFunctionExecute'] > 0, event_time_microseconds) AS compiled
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05315_%'
    GROUP BY log_comment
)
-- Comparing with the control keeps the first column valid in a build without the embedded compiler.
SELECT
    (SELECT compiled FROM shapes WHERE log_comment = '05315_kept') = (SELECT compiled FROM shapes WHERE log_comment = '05315_control'),
    (SELECT compiled FROM shapes WHERE log_comment = '05315_declined') = 0;
