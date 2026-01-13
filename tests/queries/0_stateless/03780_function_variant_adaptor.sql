SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS vf_null_only;
DROP TABLE IF EXISTS vf_one_variant_no_nulls;
DROP TABLE IF EXISTS vf_one_variant_with_nulls;
DROP TABLE IF EXISTS vf_multi_variant;
DROP TABLE IF EXISTS vf_array_one_variant_no_nulls;
DROP TABLE IF EXISTS vf_array_one_variant_with_nulls;
DROP TABLE IF EXISTS vf_array_multi_variant;
DROP TABLE IF EXISTS vf_equals;
DROP TABLE IF EXISTS vf_variant_nullable;
DROP TABLE IF EXISTS vf_two_variants_all_nulls;
DROP TABLE IF EXISTS vf_two_variants_single_no_nulls;
DROP TABLE IF EXISTS vf_two_variants_single_with_nulls;
DROP TABLE IF EXISTS vf_two_variants_multiple;

CREATE TABLE vf_null_only (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO vf_null_only VALUES (NULL), (NULL);
SELECT 'toString with only NULLs';
SELECT toString(v) FROM vf_null_only;
SELECT toTypeName(toString(v)) FROM vf_null_only LIMIT 1;

CREATE TABLE vf_one_variant_no_nulls (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO vf_one_variant_no_nulls VALUES (1), (2), (3);
SELECT 'toString with single variant type';
SELECT toString(v) FROM vf_one_variant_no_nulls;
SELECT toTypeName(toString(v)) FROM vf_one_variant_no_nulls LIMIT 1;

CREATE TABLE vf_one_variant_with_nulls (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO vf_one_variant_with_nulls VALUES (NULL), (10), (NULL), (20);
SELECT 'toString with single variant type and NULLs';
SELECT toString(v) FROM vf_one_variant_with_nulls;
SELECT toTypeName(toString(v)) FROM vf_one_variant_with_nulls LIMIT 1;

CREATE TABLE vf_multi_variant (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO vf_multi_variant VALUES (1), ('2'), (NULL), (3), ('4');
SELECT 'toString with multiple variant types';
SELECT toString(v) FROM vf_multi_variant;
SELECT toTypeName(toString(v)) FROM vf_multi_variant LIMIT 1;

SELECT 'toString with empty result';
SELECT toString(v) FROM vf_multi_variant WHERE 0;

CREATE TABLE vf_array_one_variant_no_nulls (v Variant(Array(UInt64), Array(String))) ENGINE = Memory;
INSERT INTO vf_array_one_variant_no_nulls VALUES ([10, 11]), ([20, 21, 22]);
SELECT 'arrayElement with single variant type';
SELECT arrayElement(v, 1) FROM vf_array_one_variant_no_nulls;
SELECT toTypeName(arrayElement(v, 1)) FROM vf_array_one_variant_no_nulls LIMIT 1;

CREATE TABLE vf_array_one_variant_with_nulls (v Variant(Array(UInt64), Array(String))) ENGINE = Memory;
INSERT INTO vf_array_one_variant_with_nulls VALUES (NULL), ([1, 2, 3]), (NULL), ([4, 5]);
SELECT 'arrayElement with single variant type and NULLs';
SELECT arrayElement(v, 1) FROM vf_array_one_variant_with_nulls;
SELECT toTypeName(arrayElement(v, 1)) FROM vf_array_one_variant_with_nulls LIMIT 1;

CREATE TABLE vf_array_multi_variant (v Variant(Array(UInt64), Array(String))) ENGINE = Memory;
INSERT INTO vf_array_multi_variant VALUES ([1, 2]), (['A', 'B']), (NULL), (['C']);
SELECT 'arrayElement with multiple variant types';
SELECT arrayElement(v, 1) FROM vf_array_multi_variant;
SELECT toTypeName(arrayElement(v, 1)) FROM vf_array_multi_variant LIMIT 1;

CREATE TABLE vf_equals (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO vf_equals VALUES (42), ('42'), (NULL);
SELECT 'equals with variant on left side';
SELECT toString(v), (v = CAST(42 AS UInt64)) AS is_42 FROM vf_equals;
SELECT 'equals with variant on right side';
SELECT toString(v), (CAST(42 AS UInt64) = v) AS is_42 FROM vf_equals;

CREATE TABLE vf_variant_nullable (v Variant(Float32, UInt32), x Nullable(UInt32)) ENGINE = Memory;
INSERT INTO vf_variant_nullable VALUES (42, 1), (42.42, 2), (NULL, 3), (43, NULL), (42.43, NULL), (NULL, NULL);
SELECT 'Variant + Nullable';
SELECT v + x FROM vf_variant_nullable;
SELECT toTypeName(v + x) FROM vf_variant_nullable LIMIT 1;

-- Multiple Variant arguments with nested Variant result
-- Test: Variant(UInt64, Decimal64(3)) + Variant(UInt64, Float64) -> Variant(UInt64, Decimal64(3), Float64)

CREATE TABLE vf_two_variants_all_nulls (v1 Variant(UInt64, Decimal64(3)), v2 Variant(UInt64, Float64)) ENGINE = Memory;
INSERT INTO vf_two_variants_all_nulls VALUES (NULL, NULL), (NULL, NULL);
SELECT 'Two Variants with all NULLs';
SELECT v1 + v2 FROM vf_two_variants_all_nulls;
SELECT toTypeName(v1 + v2) FROM vf_two_variants_all_nulls LIMIT 1;

CREATE TABLE vf_two_variants_single_no_nulls (v1 Variant(UInt64, Decimal64(3)), v2 Variant(UInt64, Float64)) ENGINE = Memory;
INSERT INTO vf_two_variants_single_no_nulls VALUES (10::UInt64, 20::UInt64), (15::UInt64, 25::UInt64);
SELECT 'Two Variants with single variant type, no NULLs';
SELECT v1 + v2 FROM vf_two_variants_single_no_nulls;
SELECT toTypeName(v1 + v2) FROM vf_two_variants_single_no_nulls LIMIT 1;

CREATE TABLE vf_two_variants_single_with_nulls (v1 Variant(UInt64, Decimal64(3)), v2 Variant(UInt64, Float64)) ENGINE = Memory;
INSERT INTO vf_two_variants_single_with_nulls VALUES (NULL, 5::UInt64), (10::UInt64, NULL), (15::UInt64, 20::UInt64), (NULL, NULL);
SELECT 'Two Variants with single variant type, with NULLs';
SELECT v1 + v2 FROM vf_two_variants_single_with_nulls;
SELECT toTypeName(v1 + v2) FROM vf_two_variants_single_with_nulls LIMIT 1;

CREATE TABLE vf_two_variants_multiple (v1 Variant(UInt64, Decimal64(3)), v2 Variant(UInt64, Float64)) ENGINE = Memory;
INSERT INTO vf_two_variants_multiple VALUES
    (10::UInt64, 20::UInt64),
    (5.5::Decimal64(3), 3.3::Float64),
    (NULL, 1.1::Float64),
    (10::UInt64, NULL),
    (2.2::Decimal64(3), 50::UInt64),
    (NULL, NULL);
SELECT 'Two Variants with multiple variant types';
SELECT v1 + v2 FROM vf_two_variants_multiple;
SELECT toTypeName(v1 + v2) FROM vf_two_variants_multiple LIMIT 1;

DROP TABLE vf_null_only;
DROP TABLE vf_one_variant_no_nulls;
DROP TABLE vf_one_variant_with_nulls;
DROP TABLE vf_multi_variant;
DROP TABLE vf_array_one_variant_no_nulls;
DROP TABLE vf_array_one_variant_with_nulls;
DROP TABLE vf_array_multi_variant;
DROP TABLE vf_equals;
DROP TABLE vf_variant_nullable;
DROP TABLE vf_two_variants_all_nulls;
DROP TABLE vf_two_variants_single_no_nulls;
DROP TABLE vf_two_variants_single_with_nulls;
DROP TABLE vf_two_variants_multiple;
