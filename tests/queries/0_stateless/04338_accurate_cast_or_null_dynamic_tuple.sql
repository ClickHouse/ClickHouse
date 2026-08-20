-- Regression test: accurateCastOrNull of a Tuple whose element is Dynamic/Variant must distinguish a
-- genuine source NULL from a conversion failure, matching the non-Dynamic Tuple reference. Dynamic and
-- Variant encode NULLs via the variant NULL discriminator (not a null map), so the source nulls must be
-- reconstructed -- otherwise a source NULL is misread.
--   * Nullable target element: a source NULL stays an element NULL; a conversion failure nulls the whole
--     Tuple.
--   * Non-Nullable target element: the element cannot hold NULL, so BOTH a source NULL and a conversion
--     failure null the whole Tuple (same as the non-Dynamic reference). When a block has no convertible
--     row the Dynamic/Variant conversion yields a plain default column, so the source nulls must be
--     reconstructed from the source.

SET allow_experimental_dynamic_type = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- { echo }

-- Nullable target: source NULL -> element NULL. Dynamic/Variant source must match the non-Dynamic reference.
SELECT accurateCastOrNull(CAST(tuple(NULL), 'Tuple(Dynamic)'), 'Tuple(Nullable(Float32))') AS r, toTypeName(r);
SELECT accurateCastOrNull(CAST(tuple(NULL), 'Tuple(Nullable(Float32))'), 'Tuple(Nullable(Float32))') AS r, toTypeName(r);
SELECT accurateCastOrNull(CAST(tuple(CAST(NULL, 'Variant(Float32, String)')), 'Tuple(Variant(Float32, String))'), 'Tuple(Nullable(Float32))') AS r, toTypeName(r);

-- Nullable target: successful conversion -> element value.
SELECT accurateCastOrNull(CAST(tuple(42), 'Tuple(Dynamic)'), 'Tuple(Nullable(Float32))');

-- Nullable target: parse failure -> whole Tuple NULL (not an element NULL).
SELECT accurateCastOrNull(CAST(tuple('abc'), 'Tuple(Dynamic)'), 'Tuple(Nullable(Float32))');
SELECT accurateCastOrNull(CAST(tuple(CAST('abc', 'Variant(Float32, String)')), 'Tuple(Variant(Float32, String))'), 'Tuple(Nullable(Float32))');

-- Nullable target: overflow failure -> whole Tuple NULL.
SELECT accurateCastOrNull(CAST(tuple(300::Int64), 'Tuple(Dynamic)'), 'Tuple(Nullable(UInt8))');

-- Nullable target, per-row single block: source NULL, success and parse failure together, Dynamic source.
SELECT id, accurateCastOrNull(t, 'Tuple(Nullable(Float32))') AS r
FROM (SELECT * FROM values('id UInt32, t Tuple(Dynamic)',
  (1, tuple(NULL)), (2, tuple(42::Int64)), (3, tuple('bad'::String)), (4, tuple('7'::String))))
ORDER BY id;

-- Nullable target, per-row single block: source NULL, success, parse failure together, Variant source.
SELECT id, accurateCastOrNull(t, 'Tuple(Nullable(Float32))') AS r
FROM (SELECT * FROM values('id UInt32, t Tuple(Variant(UInt8, String))',
  (1, tuple(CAST(NULL, 'Variant(UInt8, String)'))), (2, tuple(CAST(5, 'Variant(UInt8, String)'))), (3, tuple(CAST('bad', 'Variant(UInt8, String)')))))
ORDER BY id;

-- Non-Nullable target: source NULL -> whole Tuple NULL (cannot keep an element NULL). Must match the
-- non-Dynamic reference. The all-NULL block exercises the plain-default-column path.
SELECT accurateCastOrNull(CAST(tuple(NULL), 'Tuple(Dynamic)'), 'Tuple(Float32)') AS r, toTypeName(r);
SELECT accurateCastOrNull(tuple(CAST(NULL, 'Nullable(Float32)')), 'Tuple(Float32)') AS r, toTypeName(r);
SELECT accurateCastOrNull(CAST(tuple(CAST(NULL, 'Variant(Float32, String)')), 'Tuple(Variant(Float32, String))'), 'Tuple(Float32)') AS r, toTypeName(r);

-- Non-Nullable target: successful conversion -> element value.
SELECT accurateCastOrNull(CAST(tuple(42::Int64), 'Tuple(Dynamic)'), 'Tuple(Float32)');

-- Non-Nullable target, per-row single block: source NULL coexisting with a convertible row. The source
-- NULL row must be whole-Tuple NULL while the convertible row keeps its value.
SELECT id, accurateCastOrNull(t, 'Tuple(Float32)') AS r
FROM (SELECT * FROM values('id UInt32, t Tuple(Dynamic)', (1, tuple(NULL)), (2, tuple(42::Int64))))
ORDER BY id;
SELECT id, accurateCastOrNull(t, 'Tuple(UInt8)') AS r
FROM (SELECT * FROM values('id UInt32, t Tuple(Variant(UInt8, String))',
  (1, tuple(CAST(NULL, 'Variant(UInt8, String)'))), (2, tuple(CAST(5, 'Variant(UInt8, String)')))))
ORDER BY id;

-- Non-Nullable target, multi-element: a source NULL in a non-Nullable Dynamic element nulls the whole Tuple.
SELECT accurateCastOrNull(CAST((NULL, 9), 'Tuple(Dynamic, Int32)'), 'Tuple(Float32, Int32)') AS r, toTypeName(r);

-- A Variant/Dynamic element whose per-variant conversions to the SAME target type disagree on
-- nullability (e.g. UInt16 -> Float32 never fails so it stays plain, while DateTime -> Float32 can
-- fail so it becomes Nullable) must assemble cleanly. The converted columns are unified to Nullable
-- before assembly so the result column type matches every column inserted from.
SET allow_suspicious_variant_types = 1;
-- Pin the timezone so the DateTime -> Float32 epoch value below is deterministic under randomization.
SET session_timezone = 'UTC';
SELECT accurateCastOrNull(CAST(tuple(CAST('42', 'Variant(DateTime, UInt16, UInt32)')), 'Tuple(Dynamic)'), 'Tuple(Float32)');
SELECT accurateCastOrNull(CAST(tuple(CAST('42', 'Variant(DateTime, UInt16)')), 'Tuple(Dynamic)'), 'Tuple(UInt8)');
SELECT accurateCastOrNull(tuple(CAST('42', 'Variant(DateTime, UInt16)')), 'Tuple(Float32)');
-- per-row, both the numeric variant (row 1) and the temporal variant (row 2) populated in one block,
-- so the assembly inserts from both the plain and the Nullable converted column (Dynamic source).
SELECT id, accurateCastOrNull(t, 'Tuple(Float32)') AS r
FROM (SELECT 1 AS id, tuple(CAST(CAST('42', 'Variant(DateTime, UInt16)'), 'Dynamic')) AS t
      UNION ALL SELECT 2 AS id, tuple(CAST(CAST('2020-01-01 00:00:00', 'Variant(DateTime, UInt16)'), 'Dynamic')) AS t)
ORDER BY id;
-- Variant source (not via Dynamic) exercises the same heterogeneous-nullability assembly.
SELECT accurateCastOrNull(tuple(CAST('2020-01-01 00:00:00', 'Variant(DateTime, UInt16)')), 'Tuple(Float32)');
