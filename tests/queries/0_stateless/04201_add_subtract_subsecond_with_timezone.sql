-- Test: exercises `addMilliseconds`/`addMicroseconds`/`addNanoseconds` and subtract counterparts
-- with `DateTime64` + timezone argument (3-arg form).
-- Covers: src/Functions/FunctionDateOrDateTimeAddInterval.h:814 — `!WhichDataType(arguments[0].type).isDateTimeOrDateTime64()`
-- The PR widened the check from `isDateTime` to `isDateTimeOrDateTime64`, allowing
-- `DateTime64` as the 1st argument when a timezone is given. The PR's own tests cover
-- `addYears`/`Months`/`Weeks`/`Days`/`Hours`/`Minutes`/`Seconds`/`Quarters` and subtract* counterparts.
-- The subsecond variants below share the same `getReturnTypeImpl` path but are NOT tested
-- with the 3-arg form anywhere in the test suite.
SELECT addMilliseconds(toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), 10, 'Asia/Shanghai') AS v, toTypeName(v);
SELECT addMicroseconds(toDateTime64('2024-01-01 00:00:00', 6, 'UTC'), 10, 'Asia/Shanghai') AS v, toTypeName(v);
SELECT addNanoseconds(toDateTime64('2024-01-01 00:00:00', 9, 'UTC'), 10, 'Asia/Shanghai') AS v, toTypeName(v);
SELECT subtractMilliseconds(toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), 10, 'Asia/Shanghai') AS v, toTypeName(v);
SELECT subtractMicroseconds(toDateTime64('2024-01-01 00:00:00', 6, 'UTC'), 10, 'Asia/Shanghai') AS v, toTypeName(v);
SELECT subtractNanoseconds(toDateTime64('2024-01-01 00:00:00', 9, 'UTC'), 10, 'Asia/Shanghai') AS v, toTypeName(v);
