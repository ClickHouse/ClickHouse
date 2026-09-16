SELECT 'basic product';
SELECT product(x), toTypeName(product(x))
FROM VALUES('x Float64', (1.5), (2), (4));

SELECT 'negative values and zero';
SELECT product(x) FROM VALUES('x Int64', (-2), (3));
SELECT product(x) FROM VALUES('x Int64', (5), (0), (7));

SELECT 'empty and nullable input';
SELECT product(number), toTypeName(product(number)) FROM numbers(0);
SELECT toTypeName(product(x)), product(x)
FROM VALUES('x Nullable(Float64)', (1.5), (NULL), (2));
SELECT toTypeName(product(x)), product(x)
FROM VALUES('x Nullable(Float64)', (NULL), (NULL));

SELECT 'filtered input';
SELECT productIf(x, keep)
FROM VALUES('x Float64, keep UInt8', (2, 1), (3, 0), (4, 1));
SELECT productIf(x, 0)
FROM VALUES('x Float64', (2), (3), (4));

SELECT 'nullable and filtered input';
SELECT productIf(x, keep)
FROM VALUES('x Nullable(Float64), keep UInt8', (2, 1), (3, 0), (NULL, 1), (5, 1));

WITH t AS
(
    SELECT if(number % 10 = 0, NULL, 1.0) AS x, number % 2 = 0 AS keep
    FROM numbers(200000)
)
SELECT productIf(x, keep) = 1 FROM t;

SELECT 'filtered non-finite values';
SELECT isNaN(productIf(x, isFinite(x))), productIf(x, isFinite(x))
FROM VALUES('x Float64', (2), (nan), (3), (inf));

SELECT 'partial states';
SELECT productMerge(state)
FROM
(
    SELECT productState(x) AS state FROM VALUES('x Float64', (2), (3))
    UNION ALL
    SELECT productState(x) AS state FROM VALUES('x Float64', (4), (5))
);

SELECT 'window aggregation';
SELECT product(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM VALUES('x Float64', (2), (3), (4))
ORDER BY x;

SELECT 'numeric result types';
SELECT
    toTypeName(product(toInt8(number))),
    toTypeName(product(toUInt64(number))),
    toTypeName(product(toFloat32(number))),
    toTypeName(product(toFloat64(number))),
    toTypeName(product(toDecimal64(number, 2)))
FROM numbers(3);

SELECT 'bounded floating point batch reduction';
SELECT product(if(number % 2 = 0, 2., 0.5)) = 1 FROM numbers(100000);

SELECT 'floating point range';
SELECT product(if(number % 2 = 0, 1e200, 1e-200)) = 1 FROM numbers(16) SETTINGS max_threads = 1;
SELECT product(if(number % 2 = 0, 1e-200, 1e200)) = 1 FROM numbers(16) SETTINGS max_threads = 1;

SELECT 'Float64 input arithmetic';
SELECT product(x) > 0, arrayProduct(groupArray(x)) = 0
FROM VALUES('x UInt64', (9223372036854775808), (2));
