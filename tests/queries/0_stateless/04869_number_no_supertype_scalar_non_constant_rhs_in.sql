-- A scalar non-constant RHS of IN whose type and the LHS type are numbers without a
-- lossless supertype (e.g. Int64 and Float64) must be compared accurately, like the
-- constant Set path, instead of casting the RHS to the LHS type, which truncates
-- (CAST(-0.6 AS Int64) is 0). Found by SQLLogic:
-- SELECT DISTINCT * FROM tab1 WHERE NOT - + col0 + ( col0 ) NOT IN ( - - 22 / col1 + - col1 )

-- { echoOn }

SET enable_analyzer = 1;

SELECT toInt64(0) IN (materialize(toFloat64(-0.6)));
SELECT toInt64(1) IN (materialize(toFloat64(1.0)));
SELECT toInt64(0) NOT IN (materialize(toFloat64(-0.6)));
SELECT toUInt64(0) IN (materialize(toFloat64(-0.6)));
SELECT toInt128(0) IN (materialize(toFloat64(-0.6)));
SELECT toInt128(1) IN (materialize(toFloat32(1)));
SELECT toDecimal64(0.5, 2) IN (materialize(toFloat64(0.5)));
SELECT toDecimal64(0.5, 2) IN (materialize(toFloat64(-0.6)));
SELECT nullIn(toInt64(0), materialize(toFloat64(-0.6)));
SELECT notNullIn(toInt64(0), materialize(toFloat64(-0.6)));
SELECT materialize(CAST(0, 'Nullable(Int64)')) IN (materialize(CAST(-0.6, 'Nullable(Float64)')));
SELECT materialize(CAST(NULL, 'Nullable(Int64)')) IN (materialize(CAST(-0.6, 'Nullable(Float64)')));
SELECT nullIn(materialize(CAST(NULL, 'Nullable(Int64)')), materialize(CAST(-0.6, 'Nullable(Float64)')));

SELECT count() FROM (SELECT arrayJoin([14, 5, 47]) AS col1) WHERE 0 IN (22 / col1 - col1);

-- The constant Set path answers the same.
SELECT toInt64(0) IN (toFloat64(-0.6));
SELECT toInt64(1) IN (toFloat64(1.0));
SELECT toInt128(0) IN (toFloat64(-0.6));
SELECT toDecimal64(0.5, 2) IN (toFloat64(0.5));
