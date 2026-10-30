-- Nested aggregate functions whose properties set `is_window_function` (`anyRespectNulls`,
-- `anyLastRespectNulls`, the `RESPECT NULLS` modifier) are not wrapped into the outer `Null`
-- combinator by `AggregateFunctionFactory`, so the `-Sparkbar` combinator receives the bucket
-- key still wrapped in `Nullable` and must handle a `NULL` key on its own.

-- A `Nullable` key with a `Nullable` forwarded argument works and the result type stays `String`.
SELECT anyRespectNullsSparkbar(3, 0, 2)(CAST(number AS Nullable(UInt8)), toNullable(number)) AS r, toTypeName(r) FROM numbers(3);

-- A row with a `NULL` key is skipped: the middle bucket receives no rows and renders blank.
SELECT anyRespectNullsSparkbar(3, 0, 2)(CAST(if(number = 1, NULL, number) AS Nullable(UInt8)), number + 1) FROM numbers(3);

-- Only the key is filtered: a `NULL` forwarded argument still reaches the nested function, so
-- `anyLastRespectNulls` keeps its own semantics (a bucket whose last value is `NULL` renders blank),
-- while `anyLast` skips the `NULL` rows through the default `Null` combinator path.
SELECT anyLastRespectNullsSparkbar(2, 0, 1)(toUInt8(intDiv(number, 2)), if(number % 2 = 1, NULL, 5)) FROM numbers(4);
SELECT anyLastSparkbar(2, 0, 1)(toUInt8(intDiv(number, 2)), if(number % 2 = 1, NULL, 5)) FROM numbers(4);
SELECT anyLastRespectNullsSparkbar(2, 0, 1)(CAST(intDiv(number, 2) AS Nullable(UInt8)), if(number % 2 = 1, NULL, 5)) FROM numbers(4);

-- All keys `NULL`: every bucket stays empty and the result is the empty string, not `NULL`.
SELECT anyRespectNullsSparkbar(3, 0, 2)(CAST(NULL AS Nullable(UInt8)), number) AS r, toTypeName(r) FROM numbers(3);

-- The other key types are accepted in their `Nullable` form on this path as well.
SELECT anyRespectNullsSparkbar(3, -1, 1)(CAST(toInt8(number) - 1 AS Nullable(Int8)), number + 1) FROM numbers(3);
SELECT anyRespectNullsSparkbar(3, toDate('2020-01-01'), toDate('2020-01-03'))(CAST(toDate('2020-01-01') + number AS Nullable(Date)), number + 1) FROM numbers(3);
SELECT anyRespectNullsSparkbar(3, toDate32('2020-01-01'), toDate32('2020-01-03'))(CAST(toDate32('2020-01-01') + number AS Nullable(Date32)), number + 1) FROM numbers(3);
SELECT anyRespectNullsSparkbar(3, toDateTime64('2020-01-01 00:00:00', 3), toDateTime64('2020-01-01 00:00:02', 3))(CAST(toDateTime64('2020-01-01 00:00:00', 3) + number AS Nullable(DateTime64(3))), number + 1) FROM numbers(3);

-- A non-numeric `Nullable` key is still rejected.
SELECT anyRespectNullsSparkbar(3, 0, 2)(CAST(toString(number) AS Nullable(String)), number) FROM numbers(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
