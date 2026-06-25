-- Tags: no-fasttest
-- Regression test: accurateCastOrNull of a Tuple whose element is Dynamic/Variant into a Tuple
-- with a Nullable element type must distinguish a genuine source NULL from a conversion failure.
-- A source NULL stays an element NULL; a parse/overflow failure nulls the whole Tuple. Dynamic and
-- Variant encode NULLs via the variant NULL discriminator (not a null map), so the source nulls
-- must be reconstructed and subtracted -- otherwise a source NULL is misread as a conversion
-- failure and wrongly nulls the whole Tuple. The non-Dynamic Tuple(Nullable(...)) source is the
-- reference for the correct behaviour.

SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- { echo }

-- Source NULL -> element NULL. Dynamic/Variant source must match the non-Dynamic reference.
SELECT accurateCastOrNull(CAST(tuple(NULL), 'Tuple(Dynamic)'), 'Tuple(Nullable(Float32))') AS r, toTypeName(r);
SELECT accurateCastOrNull(CAST(tuple(NULL), 'Tuple(Nullable(Float32))'), 'Tuple(Nullable(Float32))') AS r, toTypeName(r);
SELECT accurateCastOrNull(CAST(tuple(CAST(NULL, 'Variant(Float32, String)')), 'Tuple(Variant(Float32, String))'), 'Tuple(Nullable(Float32))') AS r, toTypeName(r);

-- Successful conversion -> element value.
SELECT accurateCastOrNull(CAST(tuple(42), 'Tuple(Dynamic)'), 'Tuple(Nullable(Float32))');

-- Parse failure -> whole Tuple NULL (not an element NULL).
SELECT accurateCastOrNull(CAST(tuple('abc'), 'Tuple(Dynamic)'), 'Tuple(Nullable(Float32))');
SELECT accurateCastOrNull(CAST(tuple(CAST('abc', 'Variant(Float32, String)')), 'Tuple(Variant(Float32, String))'), 'Tuple(Nullable(Float32))');

-- Overflow failure -> whole Tuple NULL.
SELECT accurateCastOrNull(CAST(tuple(300::Int64), 'Tuple(Dynamic)'), 'Tuple(Nullable(UInt8))');

-- Per-row, single block: source NULL, success and parse failure together, Dynamic source.
SELECT id, accurateCastOrNull(t, 'Tuple(Nullable(Float32))') AS r
FROM (SELECT * FROM values('id UInt32, t Tuple(Dynamic)',
  (1, tuple(NULL)), (2, tuple(42::Int64)), (3, tuple('bad'::String)), (4, tuple('7'::String))))
ORDER BY id;

-- Per-row, single block: source NULL, success, parse failure together, Variant source.
SELECT id, accurateCastOrNull(t, 'Tuple(Nullable(Float32))') AS r
FROM (SELECT * FROM values('id UInt32, t Tuple(Variant(UInt8, String))',
  (1, tuple(CAST(NULL, 'Variant(UInt8, String)'))), (2, tuple(CAST(5, 'Variant(UInt8, String)'))), (3, tuple(CAST('bad', 'Variant(UInt8, String)')))))
ORDER BY id;
