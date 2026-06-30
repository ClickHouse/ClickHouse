-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 0;
SET validate_experimental_and_suspicious_types_inside_nested_types = 0;

DROP TABLE IF EXISTS test_04405_nullable_array_validation;

CREATE TABLE test_04405_nullable_array_validation
(
    x Tuple(a Nullable(Array(UInt8)))
)
ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_nullable_array_type = 1;

CREATE TABLE test_04405_nullable_array_validation
(
    id UInt8
)
ENGINE = Memory;

SET allow_experimental_nullable_array_type = 0;

ALTER TABLE test_04405_nullable_array_validation
    ADD COLUMN x Tuple(a Nullable(Array(UInt8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT *
FROM format(TSV, 'x Tuple(a Nullable(Array(UInt8)))', '')
SETTINGS
    allow_experimental_nullable_array_type = 0,
    validate_experimental_and_suspicious_types_inside_nested_types = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_nullable_array_type = 1;

DROP TABLE test_04405_nullable_array_validation;
