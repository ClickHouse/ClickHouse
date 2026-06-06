-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 0;

SELECT toTypeName(toNullable([1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(nullIf([1], [1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(JSONExtract('[]', 'Nullable(Array(UInt8))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(JSONExtract('{}', 'Tuple(Nullable(Array(UInt8)))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(JSONExtract(CAST(NULL AS Nullable(String)), 'Array(UInt8)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(defaultValueOfTypeName('Nullable(Array(UInt8))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(defaultValueOfTypeName('Tuple(Nullable(Array(UInt8)))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(accurateCastOrDefault([1], 'Nullable(Array(UInt8))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(dynamicElement(NULL::Dynamic, 'Nullable(Array(UInt8))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_nullable_array_type = 1;

SELECT toTypeName(toNullable([1]));
SELECT toTypeName(nullIf([1], [1]));
SELECT toTypeName(JSONExtract('[]', 'Nullable(Array(UInt8))'));
SELECT toTypeName(JSONExtract('{}', 'Tuple(Nullable(Array(UInt8)))'));
SELECT toTypeName(JSONExtract(CAST(NULL AS Nullable(String)), 'Array(UInt8)'));
SELECT toTypeName(defaultValueOfTypeName('Nullable(Array(UInt8))'));
SELECT toTypeName(defaultValueOfTypeName('Tuple(Nullable(Array(UInt8)))'));
SELECT toTypeName(accurateCastOrDefault([1], 'Nullable(Array(UInt8))'));
SELECT toTypeName(dynamicElement(NULL::Dynamic, 'Nullable(Array(UInt8))'));
