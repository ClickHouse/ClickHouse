-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 0;

SELECT toTypeName(toNullable([1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(nullIf([1], [1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_nullable_array_type = 1;

SELECT toTypeName(toNullable([1]));
SELECT toTypeName(nullIf([1], [1]));
