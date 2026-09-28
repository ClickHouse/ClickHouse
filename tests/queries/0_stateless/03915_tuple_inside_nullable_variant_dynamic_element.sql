-- Regardless of the setting allow_experimental_nullable_tuple_type, the output should be same.
-- The behavior is controlled by `allow_nullable_tuple_in_extracted_subcolumns` from global context.

-- { echo }

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
