SELECT firstNotDefault(NULL, 0, 42, 256) AS result;
SELECT firstNotDefault(NULL :: Nullable(UInt8), 0 :: Nullable(UInt8), 42 :: UInt8) AS result;
SELECT firstNotDefault('', '0', 'hello') AS result;
SELECT firstNotDefault(NULL::Nullable(UInt8), 0::UInt8) AS result;
SELECT firstNotDefault(false, true) AS result;

SELECT firstNotDefault([] :: Array(UInt8), [1, 2, 3] :: Array(UInt8)) AS result;
SELECT firstNotDefault(NULL::Nullable(String), ''::String, 'foo') as result, toTypeName(result);

SELECT firstNotDefault(0::UInt8, 0::UInt16, 42::UInt32) AS result, toTypeName(result);
SELECT firstNotDefault(0::Int8, 0::Int16, 42::Int32) AS result, toTypeName(result);
SELECT firstNotDefault(0::UInt32, 0::UInt64, 42::UInt128) AS result, toTypeName(result);
SELECT firstNotDefault(0::Int128, 0::Int128, 42::Int128) AS result, toTypeName(result);
SELECT firstNotDefault(0::UInt8, 0::Int8, 42::Int16) AS result, toTypeName(result);
SELECT firstNotDefault(0::Int64, 0::Int64, 42::Int64) AS result, toTypeName(result);
SELECT firstNotDefault(0.0::Float32, 0.0::Float64, 42.5::Float64) AS result, toTypeName(result);
SELECT firstNotDefault(0::Float64, 0.0::Float64, 42.0::Float64) AS result, toTypeName(result);
SELECT firstNotDefault(NULL::Nullable(Int32), 0::Nullable(Int32), 42::Nullable(Int32)) AS result, toTypeName(result);
SELECT firstNotDefault(NULL, 0::Int32, 42::Nullable(Int32)) AS result, toTypeName(result);
SELECT firstNotDefault(''::String, '0'::String, 'hello'::String) AS result, toTypeName(result);
SELECT firstNotDefault(''::FixedString(5), '0'::String, 'hello'::String) AS result, toTypeName(result);
SELECT firstNotDefault([]::Array(Int32), [0]::Array(Int32), [1, 2, 3]::Array(Int32)) AS result, toTypeName(result);
SELECT firstNotDefault([]::Array(String), ['']::Array(String), ['hello']::Array(String)) AS result, toTypeName(result);
SELECT firstNotDefault(NULL::Nullable(UInt8), 0::UInt8, 42::UInt8, 100::UInt8) AS result, toTypeName(result);
SELECT firstNotDefault(NULL::Nullable(String), ''::String, '0'::String, 'hello'::String) AS result, toTypeName(result);

SELECT firstNotDefault(NULL) AS result, toTypeName(result);
SELECT firstNotDefault(0) AS result, toTypeName(result);
SELECT firstNotDefault(''::String) AS result, toTypeName(result);
SELECT firstNotDefault([]::Array(UInt8)) AS result, toTypeName(result);

SELECT firstNotDefault(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT firstNotDefault(0, 'hello'); -- { serverError NO_COMMON_TYPE }
SELECT firstNotDefault([]::Array(UInt8), 42); -- { serverError NO_COMMON_TYPE }
SELECT firstNotDefault([]::Array(UInt8), 'hello');  -- { serverError NO_COMMON_TYPE }
SELECT firstNotDefault(0::UInt64, 1::Int64);  -- { serverError NO_COMMON_TYPE }
SELECT firstNotDefault(NULL::Nullable(Array(UInt8)), []::Array(UInt8)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT firstNotDefault(
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    number
) FROM numbers(3);

DROP TABLE IF EXISTS test_first_truthy;

CREATE TABLE test_first_truthy
(
    a Nullable(Int32),
    b Nullable(Int32),
    c Nullable(String),
    d Array(Int32)
) ENGINE = Memory;

INSERT INTO test_first_truthy VALUES
(NULL, 0, NULL, []),
(0, NULL, '', []),
(NULL, NULL, NULL, []),
(0, 0, '', []),
(1, 0, '', []),
(0, 2, '', []),
(0, 0, 'hello', []),
(0, 0, '', [1, 2, 3]);

SELECT
    a, b,
    firstNotDefault(a, b) AS result,
    toTypeName(firstNotDefault(a, b)) AS type
FROM test_first_truthy
ORDER BY ALL;

SELECT
    c,
    firstNotDefault(c, 'default'::String) AS result,
    toTypeName(firstNotDefault(c, 'default'::String)) AS type
FROM test_first_truthy
ORDER BY ALL;

SELECT
    d,
    firstNotDefault(d, [99, 100]::Array(Int32)) AS result,
    toTypeName(firstNotDefault(d, [99, 100]::Array(Int32))) AS type
FROM test_first_truthy
ORDER BY length(result);

SELECT
    a, b,
    firstNotDefault(a + b, a * b, a - b) AS result,
    toTypeName(firstNotDefault(a + b, a * b, a - b)) AS type
FROM test_first_truthy
ORDER BY ALL;

SELECT
    a, b,
    firstNotDefault(42, a, b) AS result1,
    firstNotDefault(0, a, b) AS result2,
    firstNotDefault(NULL, a, b) AS result3
FROM test_first_truthy
ORDER BY ALL;

DROP TABLE test_first_truthy;
