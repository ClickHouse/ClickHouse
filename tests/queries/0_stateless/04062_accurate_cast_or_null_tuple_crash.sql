-- accurateCastOrNull to Tuple types (e.g. Point) crashed the server with
-- "Bad cast from type DB::ColumnNullable to DB::ColumnVector<double>"
-- because element wrappers produced ColumnNullable columns inside the tuple,
-- but the declared type Nullable(Tuple(Float64, Float64)) expects non-nullable
-- tuple elements with Nullable only at the outer level.

-- Basic: non-null tuple to Point
SELECT accurateCastOrNull(tuple(1, 2), 'Point');

-- Tuple with NULL element should produce outer NULL
SELECT accurateCastOrNull(tuple(NULL, 1), 'Point');

-- NULL input
SELECT accurateCastOrNull(NULL, 'Point');

-- Verify result type
SELECT toTypeName(accurateCastOrNull(tuple(1.5, 2.5), 'Point'));

-- Verify non-null result is accessible
SELECT tupleElement(accurateCastOrNull(tuple(3, 4), 'Point'), 1), tupleElement(accurateCastOrNull(tuple(3, 4), 'Point'), 2);

-- Target type with intentionally nullable elements: must preserve inner Nullable
SELECT accurateCastOrNull(tuple(1, 2), 'Tuple(Nullable(Float64), Nullable(Float64))');
SELECT toTypeName(accurateCastOrNull(tuple(1, 2), 'Tuple(Nullable(Float64), Nullable(Float64))'));

-- Original fuzzer crash query (simplified)
SELECT accurateCastOrNull((SELECT modulo(intDiv(intDiv(plus((SELECT DISTINCT toInt128(2147483646) QUALIFY minus(256, -1) LIMIT 100, 7), accurateCastOrNull('\'', 'Int8')), (SELECT 1023 GROUP BY 1)), intDiv(-2147483648, 100)), NULL), -9223372036854775808 LIMIT -2147483647), 'Point');
