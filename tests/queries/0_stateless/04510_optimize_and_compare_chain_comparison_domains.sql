SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;
SET optimize_and_compare_chain_max_hash_work = 0;
SET allow_experimental_time_time64_type = 1;

-- Comparisons involving different type pairs do not necessarily use the same ordering.
-- The optimizer must not connect such pairs into a transitive chain.

-- `Decimal` to integer is exact, while `Decimal` to `Float64` converts both operands to
-- `Float64`. The inferred endpoint comparison used to round `a` up and reject this row.
SELECT 'decimal float rounding', count()
FROM values(
    'a Decimal64(1), b Int64',
    ('36028797018963966.9', 36028797018963967))
WHERE a < b AND b < toFloat64(36028797018963968);

-- Checking only the generated endpoint types is insufficient. The endpoints here are native
-- numbers, but the unsafe `Decimal` intermediate still makes the inferred comparison false.
SELECT 'unsafe intermediate', count()
FROM values(
    'i Int64, d Decimal64(1)',
    (36028797018963969, '36028797018963969.1'))
WHERE i < d AND d <= toFloat64(36028797018963968);

-- A constant `String` is converted independently to the type of the other operand. `256` fits
-- into `UInt16` but not into `UInt8`, so the implied endpoint comparison is false.
SELECT 'constant string conversion', count()
FROM values('a UInt16, b UInt8', (200, 100))
WHERE '256' > a AND a > b;

-- Both original comparisons have a common type, but the inferred `Time` to `Date` endpoint does
-- not. The optimization used to turn this valid query into an exception during analysis.
SELECT 'incompatible endpoint', count()
FROM values(
    't Time, dt DateTime(\'UTC\')',
    ('01:00:00', '1970-01-01 02:00:00'))
WHERE t < dt AND dt < toDate('1970-01-02');

-- Keep deriving conditions inside the two explicitly supported domains.
SELECT
    'safe domains remain optimized',
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Int32, b Int64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a Int32, b Int64', (1, 2))
           WHERE a < b AND b < toFloat64(3)
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%'),
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a String, b String', ('a', 'b'))
           WHERE a < b AND b < 'z'
           SETTINGS optimize_and_compare_chain = 1)
     WHERE explain LIKE '%function_name: less,%')
        >
    (SELECT count()
     FROM (EXPLAIN QUERY TREE
           SELECT * FROM values('a String, b String', ('a', 'b'))
           WHERE a < b AND b < 'z'
           SETTINGS optimize_and_compare_chain = 0)
     WHERE explain LIKE '%function_name: less,%');
