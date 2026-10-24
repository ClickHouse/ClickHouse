-- Test case extracted from AST fuzzer failure
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=95226&sha=4092c976e68130b7d6993e747e14ed6645efd0fa&name_0=PR&name_1=AST%20fuzzer%20%28amd_msan%29
-- MemorySanitizer: use-of-uninitialized-value in CRC32Hash during nullable_serialized aggregation
-- The bug was in ColumnNullable::getSerializedValueSize which returned 1 + nested_size even for NULL values,
-- but serializeValueIntoMemory only writes 1 byte for NULLs, leaving uninitialized memory.

SET allow_experimental_nullable_tuple_type = 1;

-- Minimal reproducer
SELECT CAST(tuple(NULL, NULL), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))')
GROUP BY 1, tuple(1, NULL);

-- Original failing query from AST fuzzer
SELECT
    tuple(tuple(materialize(NULL)), 42, CAST(tuple(NULL, NULL), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))'), toNullable(NULL)),
    (NULL, NULL) IN CAST(tuple(NULL, 1), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))')
GROUP BY
    1,
    isZeroOrNull(assumeNotNull(materialize(NULL))),
    tuple(1, NULL),
    (NULL, NULL) GLOBAL IN CAST(tuple(NULL, NULL), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))')
SETTINGS transform_null_in = 0, enable_analyzer = 1;

SELECT
    tuple(tuple(materialize(NULL)), 42, CAST(tuple(NULL, NULL), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))'), toNullable(NULL)),
    (NULL, NULL) IN CAST(tuple(NULL, 1), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))')
GROUP BY
    1,
    isZeroOrNull(assumeNotNull(materialize(NULL))),
    tuple(1, NULL),
    (NULL, NULL) GLOBAL IN CAST(tuple(NULL, NULL), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))')
SETTINGS transform_null_in = 0, enable_analyzer = 0; -- { serverError ILLEGAL_COLUMN }

-- Multiple nullable tuples in GROUP BY
SELECT count()
FROM (SELECT number % 3 AS a, number % 5 AS b FROM numbers(100))
GROUP BY
    CAST(tuple(a, b), 'Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))'),
    tuple(1, NULL)
ORDER BY 1;

-- Nested nullable tuples
SELECT CAST(tuple(NULL, tuple(NULL, NULL)), 'Nullable(Tuple(Nullable(UInt32), Nullable(Tuple(Nullable(UInt32), Nullable(UInt32)))))')
GROUP BY 1;

-- Mix of null and non-null values
SELECT t, count() FROM (
    SELECT
        CAST(if(number % 2 = 0, tuple(number, number * 2), NULL), 'Nullable(Tuple(Nullable(UInt64), Nullable(UInt64)))') AS t
    FROM numbers(10)
) GROUP BY t
ORDER BY t NULLS FIRST;
