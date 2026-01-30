SET enable_variant_type = 1;
SET allow_suspicious_variant_types = 1;

-- Test default behavior: type mismatch returns NULL
SET variant_throw_on_type_mismatch = 0;

SELECT 'Default behavior: type mismatch returns NULL';

-- Simple case: Variant(UInt64, String) + UInt8
-- String values should return NULL when added to a number
SELECT number % 2 ? number : 'even' AS x, x + 1 AS result
FROM numbers(6)
ORDER BY number;

SELECT 'Type of result';
SELECT toTypeName(x + 1)
FROM (SELECT number % 2 ? number : 'even' AS x FROM numbers(1));

-- Test with table
DROP TABLE IF EXISTS test_variant_type_mismatch;
CREATE TABLE test_variant_type_mismatch (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test_variant_type_mismatch VALUES (10), ('hello'), (NULL), (20), ('world');

SELECT 'Table test: plus operation with type mismatch';
SELECT v, v + 5 AS result FROM test_variant_type_mismatch ORDER BY v;

SELECT 'Type of table result';
SELECT toTypeName(v + 5) FROM test_variant_type_mismatch LIMIT 1;

-- Test with multiple operations
SELECT 'Multiple operations with type mismatch';
SELECT v, v * 2 AS mult, v - 1 AS sub FROM test_variant_type_mismatch ORDER BY v;

DROP TABLE test_variant_type_mismatch;

-- Test strict behavior: type mismatch throws exception
SET variant_throw_on_type_mismatch = 1;

SELECT 'Strict behavior: type mismatch throws exception';

-- This should work fine (all compatible types)
DROP TABLE IF EXISTS test_variant_compatible;
CREATE TABLE test_variant_compatible (v Variant(UInt64, Float64)) ENGINE = Memory;
INSERT INTO test_variant_compatible VALUES (10), (3.14), (NULL), (20);

SELECT 'Strict mode with compatible types';
SELECT v, v + 1 AS result FROM test_variant_compatible ORDER BY v;

DROP TABLE test_variant_compatible;

-- This should throw an exception (incompatible type)
SELECT 'Attempting operation with incompatible type (should fail)';
SELECT number % 2 ? number : 'even' AS x, x + 1 AS result FROM numbers(2); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
