-- Tests that accurateCastOrNull / accurateCastOrDefault do not produce a logical error when a
-- Dynamic or Variant column is nested inside a container (Tuple) and the target element type is
-- not Nullable. The conversion of the inner Dynamic/Variant returns a Nullable column to signal
-- values that could not be converted, while the result column used to be built from the bare
-- (non-Nullable) element type, causing a column type mismatch in insertFrom.

SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- { echo }

-- Dynamic inside a Tuple, accurateCastOrNull to a non-Nullable element type.
SELECT accurateCastOrNull(CAST(tuple(1), 'Tuple(Dynamic)'), 'Tuple(Float32)') AS r, toTypeName(r);
-- A value that does not fit makes the whole Tuple NULL.
SELECT accurateCastOrNull(CAST(tuple(256), 'Tuple(Dynamic)'), 'Tuple(UInt8)') AS r, toTypeName(r);

-- accurateCastOrDefault goes through the same accurateOrNull path internally.
SELECT accurateCastOrDefault(CAST(tuple(1), 'Tuple(Dynamic)'), 'Tuple(Float32)') AS r, toTypeName(r);
SELECT accurateCastOrDefault(CAST(tuple(256), 'Tuple(Dynamic)'), 'Tuple(UInt8)') AS r, toTypeName(r);

-- Variant inside a Tuple, multiple variant types across rows (assembles the result row by row).
SELECT accurateCastOrNull(tuple(v), 'Tuple(Float32)') AS r
FROM (SELECT number, multiIf(number % 3 = 0, number, number % 3 = 1, 'str_' || toString(number), range(number)) AS v FROM numbers(6)) ORDER BY number;

-- Dynamic inside a Tuple, multiple variant types across rows.
SELECT accurateCastOrNull(tuple(d), 'Tuple(Float32)') AS r
FROM (SELECT number, multiIf(number % 3 = 0, number, number % 3 = 1, 'str_' || toString(number), range(number))::Dynamic AS d FROM numbers(6)) ORDER BY number;

-- Standalone Variant conversion still works (target type is already Nullable here).
SELECT accurateCastOrNull(v, 'Float32') AS r
FROM (SELECT number, multiIf(number % 3 = 0, number, number % 3 = 1, 'str_' || toString(number), range(number)) AS v FROM numbers(6)) ORDER BY number;
