-- Adding a floating point number of seconds to a value that can represent a fraction of a second must
-- keep that fraction instead of truncating the delta to whole seconds.
-- https://github.com/ClickHouse/ClickHouse/issues/50440

SELECT '-- plus and minus on DateTime64';
WITH toDateTime64('2023-06-01 18:10:01.212348', 6, 'UTC') AS t
SELECT t + 0.1, t - 0.1, t + 1.5, t - 1.5, toTypeName(t + 0.1);

SELECT '-- every combination of constant and materialized arguments';
WITH toDateTime64('2023-06-01 18:10:01.212348', 6, 'UTC') AS t
SELECT t + 0.1, materialize(t) + 0.1, t + materialize(0.1), materialize(t) + materialize(0.1);

WITH toDateTime64('2023-06-01 18:10:01.212348', 6, 'UTC') AS t
SELECT t - 0.1, materialize(t) - 0.1, t - materialize(0.1), materialize(t) - materialize(0.1);

SELECT '-- the number may also come first';
WITH toDateTime64('2023-06-01 18:10:01.212348', 6, 'UTC') AS t
SELECT 0.1 + t, 0.1 + materialize(t), materialize(0.1) + t;

SELECT '-- addSeconds and subtractSeconds';
WITH toDateTime64('2023-06-01 18:10:01.212348', 6, 'UTC') AS t
SELECT addSeconds(t, 0.1), subtractSeconds(t, 0.1), addSeconds(materialize(t), 0.1), addSeconds(t, materialize(0.1));

SELECT '-- decimal literals a truncating implementation would get wrong';
WITH toDateTime64('2023-06-01 18:10:01.000000', 6, 'UTC') AS t
SELECT t + 0.3, t + 0.7, t + 0.29, t + 0.07;

SELECT '-- every scale';
SELECT toDateTime64('2023-06-01 18:10:01', 0, 'UTC') + 1.5;
SELECT toDateTime64('2023-06-01 18:10:01.5', 1, 'UTC') + 0.5;
SELECT toDateTime64('2023-06-01 18:10:01.123', 3, 'UTC') + 0.001;
SELECT toDateTime64('2023-06-01 18:10:01.123456', 6, 'UTC') + 0.000001;
SELECT toDateTime64('2023-06-01 18:10:01.123456789', 9, 'UTC') + 0.000000001;

SELECT '-- Float32 delta';
SELECT toDateTime64('2023-06-01 18:10:01.000000', 6, 'UTC') + toFloat32(0.5);

SELECT '-- a whole number of seconds given as a float is still exact';
SELECT toDateTime64('2023-06-01 18:10:01.123456789', 9, 'UTC') + 1234567890.0;
SELECT toDateTime64('2023-06-01 18:10:01.123456789', 9, 'UTC') + 1234567890;

SELECT '-- Time64';
WITH toTime64('18:10:01.212348', 6) AS t
SELECT t + 0.1, t - 0.1, materialize(t) + 0.1, toTypeName(t + 0.1);

SELECT '-- a String argument is parsed into a DateTime64(3)';
SELECT addSeconds('2023-06-01 18:10:01.123', 0.5), subtractSeconds('2023-06-01 18:10:01.123', 0.5);
SELECT addSeconds(materialize('2023-06-01 18:10:01.123'), 0.5);

SELECT '-- a column of deltas';
SELECT toDateTime64('2023-06-01 18:10:01.000000', 6, 'UTC') + (number / 10) FROM numbers(4);

SELECT '-- the types that cannot represent a fraction of a second are unchanged';
SELECT toDateTime('2023-06-01 18:10:01', 'UTC') + 0.9 AS t, toTypeName(t);
SELECT toDate('2023-06-01') + 0.9 AS t, toTypeName(t);
SELECT toDate32('2023-06-01') + 0.9 AS t, toTypeName(t);
SELECT toTime('18:10:01') + 0.9 AS t, toTypeName(t);

SELECT '-- an out of range delta is rejected instead of being undefined';
SELECT toDateTime64('2023-06-01 18:10:01.123456', 6, 'UTC') + 1e30; -- { serverError DECIMAL_OVERFLOW }
SELECT toDateTime64('2023-06-01 18:10:01.123456', 6, 'UTC') + nan; -- { serverError DECIMAL_OVERFLOW }
SELECT toDateTime64('2023-06-01 18:10:01.123456', 6, 'UTC') + inf; -- { serverError DECIMAL_OVERFLOW }
SELECT toDateTime64('2023-06-01 18:10:01.123456', 6, 'UTC') + materialize(1e30); -- { serverError DECIMAL_OVERFLOW }
