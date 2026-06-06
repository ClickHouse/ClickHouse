-- Tags: no-random-settings

DROP TABLE IF EXISTS nullable_array_inferred_default_null_modifier;
DROP TABLE IF EXISTS nullable_array_inferred_default_setting;

SET allow_experimental_nullable_array_type = 0;
SET data_type_default_nullable = 0;

CREATE TABLE nullable_array_inferred_default_null_modifier (a DEFAULT [] NULL) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET data_type_default_nullable = 1;

CREATE TABLE nullable_array_inferred_default_setting (a DEFAULT []) ENGINE = Memory; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_nullable_array_type = 1;
SET data_type_default_nullable = 0;

CREATE TABLE nullable_array_inferred_default_null_modifier (a DEFAULT [1] NULL) ENGINE = Memory;
SELECT throwIf(groupArray(type) != ['Nullable(Array(UInt8))'])
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'nullable_array_inferred_default_null_modifier'
    AND name = 'a'
FORMAT Null;
DROP TABLE nullable_array_inferred_default_null_modifier;

SET data_type_default_nullable = 1;

CREATE TABLE nullable_array_inferred_default_setting (a DEFAULT [1]) ENGINE = Memory;
SELECT throwIf(groupArray(type) != ['Nullable(Array(UInt8))'])
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'nullable_array_inferred_default_setting'
    AND name = 'a'
FORMAT Null;
DROP TABLE nullable_array_inferred_default_setting;

SELECT 'ok';
