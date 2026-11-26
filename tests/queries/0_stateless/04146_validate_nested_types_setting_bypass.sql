-- Test: exercises validateDataType bypass when validate_experimental_and_suspicious_types_inside_nested_types=0
-- Covers: src/Interpreters/parseColumnsListForTableFunction.cpp:159 — `if (settings.validate_nested_types)` branch
-- The PR adds the bypass setting (default true). When set to false, forEachChild() recursive
-- validation is skipped, allowing suspicious types nested under Array/Map/Tuple/Variant
-- (restoring pre-#59385 behaviour). Top-level validation still runs.

SET allow_suspicious_low_cardinality_types = 0;
SET allow_suspicious_fixed_string_types = 0;
SET allow_suspicious_variant_types = 0;

-- 1. Default behaviour (validate_nested_types = 1): nested suspicious types are rejected.
SELECT [42]::Array(LowCardinality(UInt64)) SETTINGS validate_experimental_and_suspicious_types_inside_nested_types = 1; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
SELECT ['x']::Array(FixedString(1000000)) SETTINGS validate_experimental_and_suspicious_types_inside_nested_types = 1; -- { serverError ILLEGAL_COLUMN }

-- 2. With bypass (validate_nested_types = 0): nested suspicious types are accepted.
SET validate_experimental_and_suspicious_types_inside_nested_types = 0;

SELECT [42]::Array(LowCardinality(UInt64));
SELECT map('a', 42)::Map(String, LowCardinality(UInt64));
SELECT tuple('a', 42)::Tuple(String, LowCardinality(UInt64));
SELECT [[[42]]]::Array(Array(Array(LowCardinality(UInt64))));

DROP TABLE IF EXISTS test_nested_bypass;
CREATE TABLE test_nested_bypass (x Array(LowCardinality(UInt64))) ENGINE = Memory;
INSERT INTO test_nested_bypass VALUES ([1, 2, 3]);
SELECT * FROM test_nested_bypass ORDER BY x;
DROP TABLE test_nested_bypass;

CREATE TABLE test_nested_bypass (x Tuple(s String, fs FixedString(1000000))) ENGINE = Memory;
DROP TABLE test_nested_bypass;

-- 3. Bypass does NOT skip top-level validation: top-level suspicious types still rejected.
SELECT 1::LowCardinality(UInt64); -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
SELECT ''::FixedString(1000000); -- { serverError ILLEGAL_COLUMN }

-- 4. ALTER path (AlterCommands.cpp also calls validateDataType) honours the bypass.
DROP TABLE IF EXISTS test_alter_nested_bypass;
CREATE TABLE test_alter_nested_bypass (id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE test_alter_nested_bypass ADD COLUMN bad Array(LowCardinality(UInt64));
DESCRIBE TABLE test_alter_nested_bypass;
DROP TABLE test_alter_nested_bypass;
