-- Tuple-related queries from tests/queries/0_stateless/03915_tuple_inside_nullable_variant_dynamic_element.sql.

SET allow_experimental_nullable_tuple_type = 0;

SELECT toTypeName(variantElement(v, 'Tuple(UInt64, String)')), variantElement(v, 'Tuple(UInt64, String)')
FROM (SELECT CAST(toUInt64(42), 'Variant(Tuple(UInt64, String), UInt64)') AS v);
SELECT toTypeName(variantElement(v, 'Tuple(UInt64, String)')), variantElement(v, 'Tuple(UInt64, String)')
FROM (SELECT CAST(tuple(toUInt64(1), 'x'), 'Variant(Tuple(UInt64, String), UInt64)') AS v);
SELECT toTypeName(dynamicElement(d, 'Tuple(UInt64, String)')), dynamicElement(d, 'Tuple(UInt64, String)')
FROM (SELECT CAST(toUInt64(42), 'Dynamic') AS d);
SELECT toTypeName(dynamicElement(d, 'Tuple(UInt64, String)')), dynamicElement(d, 'Tuple(UInt64, String)')
FROM (SELECT CAST(tuple(toUInt64(1), 'x'), 'Dynamic') AS d);

SET allow_experimental_nullable_tuple_type = 1;

SELECT toTypeName(variantElement(v, 'Tuple(UInt64, String)')), variantElement(v, 'Tuple(UInt64, String)')
FROM (SELECT CAST(toUInt64(42), 'Variant(Tuple(UInt64, String), UInt64)') AS v);
SELECT toTypeName(variantElement(v, 'Tuple(UInt64, String)')), variantElement(v, 'Tuple(UInt64, String)')
FROM (SELECT CAST(tuple(toUInt64(1), 'x'), 'Variant(Tuple(UInt64, String), UInt64)') AS v);
SELECT toTypeName(dynamicElement(d, 'Tuple(UInt64, String)')), dynamicElement(d, 'Tuple(UInt64, String)')
FROM (SELECT CAST(toUInt64(42), 'Dynamic') AS d);
SELECT toTypeName(dynamicElement(d, 'Tuple(UInt64, String)')), dynamicElement(d, 'Tuple(UInt64, String)')
FROM (SELECT CAST(tuple(toUInt64(1), 'x'), 'Dynamic') AS d);
