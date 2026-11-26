-- `transform` used to convert its constant default value through `convertFieldToType`,
-- which drops the source type and silently falls back to raw numeric conversion. That
-- produces wrong results in three families: `Date`/`Date32` -> `DateTime`/`DateTime64`
-- treats days as seconds, `Enum` -> `String` emits the underlying integer instead of
-- the value name, and `FixedString` -> `String` keeps trailing NULs. `caseWithExpression`
-- inherits the same bug because it rewrites to `transform` when all WHEN/THEN values
-- are constant. The fix routes the default through `castColumn`, like the other arrays.

SET session_timezone = 'UTC';

-- Group B: Date / Date32 default, DateTime / DateTime64 result type.
SELECT transform(materialize(0), [1], [toDateTime64(0, 6)], CAST('2149-06-04' AS Date));
SELECT transform(materialize(0), [1], [toDateTime64(0, 6)], materialize(CAST('2149-06-04' AS Date)));
SELECT transform(materialize(0), [1], [toDateTime64(0, 6)], CAST('2149-06-04' AS Date32));
SELECT transform(materialize(0), [1], [toDateTime64(0, 6)], materialize(CAST('2149-06-04' AS Date32)));
SELECT transform(materialize(0), [1], [toDateTime(0)], CAST('2105-06-15' AS Date));
SELECT transform(materialize(0), [1], [toDateTime(0)], materialize(CAST('2105-06-15' AS Date)));
SELECT transform(materialize(0), [1], [toDateTime(0)], CAST('2105-06-15' AS Date32));
SELECT transform(materialize(0), [1], [toDateTime(0)], materialize(CAST('2105-06-15' AS Date32)));
-- Scale=0 corner case: no scale multiplier, but the days-as-seconds substitution still shows.
SELECT transform(materialize(0), [1], [toDateTime64(0, 0)], CAST('2105-06-15' AS Date));

-- Group A: Enum default, String / FixedString result type.
SELECT transform(materialize(0), [1], [CAST('x' AS String)], CAST('cV5' AS Enum16('cV5' = -24408)));
SELECT transform(materialize(0), [1], [CAST('x' AS String)], materialize(CAST('cV5' AS Enum16('cV5' = -24408))));
SELECT transform(materialize(0), [1], [CAST('x' AS FixedString(10))], CAST('cV1' AS Enum8('cV1' = 125)));
SELECT transform(materialize(0), [1], [CAST('x' AS FixedString(10))], materialize(CAST('cV1' AS Enum8('cV1' = 125))));

-- Group C: FixedString default, String result type. Materialized path trims trailing NULs;
-- the const path used to keep them.
SELECT length(transform(materialize(0), [1], [CAST('x' AS String)], CAST(toFixedString('', 23) AS FixedString(23))));
SELECT length(transform(materialize(0), [1], [CAST('x' AS String)], materialize(CAST(toFixedString('', 23) AS FixedString(23)))));
SELECT transform(materialize(0), [1], [CAST('x' AS String)], CAST(toFixedString('1972-9-5', 54) AS FixedString(54)));
SELECT transform(materialize(0), [1], [CAST('x' AS String)], materialize(CAST(toFixedString('1972-9-5', 54) AS FixedString(54))));

-- `caseWithExpression` reproductions of each group. Routes through `transform`
-- when WHEN/THEN values are constant.
SELECT caseWithExpression(materialize(CAST('1900-01-01' AS Date32)),
                          CAST(0 AS Int8),
                          CAST('1970-01-01 01:00:00.000000' AS DateTime64(6)),
                          CAST('2149-06-04' AS Date));
SELECT caseWithExpression(materialize(CAST('1900-01-01' AS Date32)),
                          CAST(0 AS Int8),
                          CAST('1970-01-01 01:00:00.000000' AS DateTime64(6)),
                          materialize(CAST('2149-06-04' AS Date)));
SELECT caseWithExpression(materialize(CAST(-18 AS Int16)),
                          CAST('cV8' AS Enum16('cV8' = -27027, 'cV6' = -3271)),
                          CAST(NULL AS Nullable(String)),
                          CAST('cV6' AS Enum16('cV6' = 28871)));
SELECT caseWithExpression(materialize(CAST(-18 AS Int16)),
                          CAST('cV8' AS Enum16('cV8' = -27027, 'cV6' = -3271)),
                          CAST(NULL AS Nullable(String)),
                          materialize(CAST('cV6' AS Enum16('cV6' = 28871))));
