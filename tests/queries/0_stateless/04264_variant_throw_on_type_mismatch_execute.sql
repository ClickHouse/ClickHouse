-- Test that variant_throw_on_type_mismatch=false returns NULL (not an exception) for functions
-- that pass build() but throw during execute(), such as comparison functions.
-- https://github.com/ClickHouse/ClickHouse/issues/103484

SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET variant_throw_on_type_mismatch = false;

-- Variant tests

-- Path 1: single variant, no NULLs (getGlobalDiscriminatorOfOneNoneEmptyVariantNoNulls)
SELECT 'variant: single variant no nulls';
DROP TABLE IF EXISTS test_v1;
CREATE TABLE test_v1 (id UInt32, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test_v1 VALUES (1, 'hello'), (2, 'world');
SELECT v = 42 FROM test_v1 ORDER BY id;
SELECT v != 42 FROM test_v1 ORDER BY id;
DROP TABLE test_v1;

-- Path 2: single variant with NULLs (getGlobalDiscriminatorOfOneNoneEmptyVariant)
SELECT 'variant: single variant with nulls';
DROP TABLE IF EXISTS test_v2;
CREATE TABLE test_v2 (id UInt32, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test_v2 VALUES (1, 'hello'), (2, NULL);
SELECT v = 42 FROM test_v2 ORDER BY id;
SELECT v != 42 FROM test_v2 ORDER BY id;
DROP TABLE test_v2;

-- Path 3: multiple variants (general case)
SELECT 'variant: multiple variants';
DROP TABLE IF EXISTS test_v3;
CREATE TABLE test_v3 (id UInt32, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test_v3 VALUES (1, 42), (2, 'hello'), (3, NULL), (4, 10);
SELECT v = 42 FROM test_v3 ORDER BY id;
SELECT v != 42 FROM test_v3 ORDER BY id;
DROP TABLE test_v3;

-- Verify that arithmetic still works correctly with the setting
SELECT 'variant: arithmetic still works';
DROP TABLE IF EXISTS test_v4;
CREATE TABLE test_v4 (id UInt32, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test_v4 VALUES (1, 42), (2, 'hello'), (3, NULL), (4, 10);
SELECT v + 5 FROM test_v4 ORDER BY id;
DROP TABLE test_v4;

-- Dynamic tests

SET allow_experimental_dynamic_type = 1;
SET dynamic_throw_on_type_mismatch = false;

-- Path 1: single variant, no NULLs
SELECT 'dynamic: single variant no nulls';
DROP TABLE IF EXISTS test_d1;
CREATE TABLE test_d1 (id UInt32, v Dynamic) ENGINE = Memory;
INSERT INTO test_d1 VALUES (1, 'hello'::String), (2, 'world'::String);
SELECT v = 42 FROM test_d1 ORDER BY id;
DROP TABLE test_d1;

-- Path 2: single variant with NULLs
SELECT 'dynamic: single variant with nulls';
DROP TABLE IF EXISTS test_d2;
CREATE TABLE test_d2 (id UInt32, v Dynamic) ENGINE = Memory;
INSERT INTO test_d2 VALUES (1, 'hello'::String), (2, NULL);
SELECT v = 42 FROM test_d2 ORDER BY id;
DROP TABLE test_d2;

-- Path 3: multiple variants (general case)
SELECT 'dynamic: multiple variants';
DROP TABLE IF EXISTS test_d3;
CREATE TABLE test_d3 (id UInt32, v Dynamic) ENGINE = Memory;
INSERT INTO test_d3 VALUES (1, 42::UInt64), (2, 'hello'::String), (3, NULL), (4, 10::UInt64);
SELECT v = 42 FROM test_d3 ORDER BY id;
SELECT v + 5 FROM test_d3 ORDER BY id;
DROP TABLE test_d3;

-- Verify that with throw_on_type_mismatch=true (default), exceptions still fire
SET variant_throw_on_type_mismatch = true;
SET dynamic_throw_on_type_mismatch = true;

SELECT 'variant: throws with default setting';
DROP TABLE IF EXISTS test_v5;
CREATE TABLE test_v5 (id UInt32, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test_v5 VALUES (1, 42), (2, 'hello');
SELECT v = 42 FROM test_v5 ORDER BY id; -- {serverError NO_COMMON_TYPE}
DROP TABLE test_v5;

SELECT 'dynamic: throws with default setting';
DROP TABLE IF EXISTS test_d4;
CREATE TABLE test_d4 (id UInt32, v Dynamic) ENGINE = Memory;
INSERT INTO test_d4 VALUES (1, 42::UInt64), (2, 'hello'::String);
SELECT v = 42 FROM test_d4 ORDER BY id; -- {serverError NO_COMMON_TYPE}
DROP TABLE test_d4;
