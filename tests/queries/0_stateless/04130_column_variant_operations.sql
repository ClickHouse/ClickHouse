-- Exercise Variant column operations: construction, variantElement, variantType,
-- null handling, ORDER BY across types, GROUP BY across types, round-trip through
-- Memory and insert of scalar / array / null values. Covers ColumnVariant.cpp.

DROP TABLE IF EXISTS variant_mem;
CREATE TABLE variant_mem (v Variant(Int32, String, Array(Int32))) ENGINE = Memory;
INSERT INTO variant_mem VALUES
    (1::Int32),
    (42::Int32),
    ('hello'),
    ('world'),
    ([1, 2, 3]),
    ([]::Array(Int32)),
    (NULL);

SELECT '--- variantType per row ---';
SELECT variantType(v) FROM variant_mem ORDER BY variantType(v), toString(v);

SELECT '--- variantElement (each arm) ---';
SELECT variantElement(v, 'Int32') FROM variant_mem ORDER BY variantType(v), toString(v);
SELECT variantElement(v, 'String') FROM variant_mem ORDER BY variantType(v), toString(v);
SELECT variantElement(v, 'Array(Int32)') FROM variant_mem ORDER BY variantType(v), toString(v);

SELECT '--- variantElement with default value ---';
SELECT variantElement(v, 'Int32', -1) FROM variant_mem ORDER BY variantType(v), toString(v);

SELECT '--- null handling (isNull / isNotNull) ---';
SELECT isNull(v), isNotNull(v) FROM variant_mem ORDER BY variantType(v), toString(v);

SELECT '--- count by variant type ---';
SELECT variantType(v), count() FROM variant_mem GROUP BY variantType(v) ORDER BY 1;

SELECT '--- MergeTree round-trip ---';
DROP TABLE IF EXISTS variant_mt;
CREATE TABLE variant_mt (id UInt32, v Variant(Int32, String, Array(Int32))) ENGINE = MergeTree ORDER BY id;
INSERT INTO variant_mt VALUES (1, 1::Int32), (2, 'hi'), (3, [10, 20]), (4, NULL);
OPTIMIZE TABLE variant_mt FINAL;
SELECT id, variantType(v), v FROM variant_mt ORDER BY id;

SELECT '--- sort/compare across arms (type first, then value) ---';
SELECT v FROM variant_mem WHERE v IS NOT NULL ORDER BY v LIMIT 10 SETTINGS allow_suspicious_types_in_order_by = 1;

SELECT '--- nested Variant via array of Variant ---';
SELECT arraySort(x -> toString(x), [CAST(1::Int32, 'Variant(Int32, String)'), CAST('hi', 'Variant(Int32, String)')]);

-- Variant types cannot be wrapped in Nullable (Variant has its own null arm).
SELECT '--- Variant already models null (no Nullable wrapping) ---';
SELECT CAST(1::Int32 AS Variant(Int32, String));
SELECT CAST(NULL AS Variant(Int32, String));

SELECT '--- error: CAST from non-member type ---';
SELECT CAST(1::UInt8 AS Variant(Int32, String)); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '--- error: variantElement with unknown type ---';
SELECT variantElement(v, 'Int64') FROM variant_mem LIMIT 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- filter on variant arm ---';
SELECT v FROM variant_mem WHERE variantType(v) = 'String' ORDER BY toString(v);
SELECT v FROM variant_mem WHERE variantElement(v, 'Int32') IS NOT NULL ORDER BY toString(v);

SELECT '--- Distinct over Variant ---';
SELECT DISTINCT variantType(v) FROM variant_mem ORDER BY 1;

DROP TABLE variant_mem;
DROP TABLE variant_mt;
