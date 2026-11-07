SELECT 'basic product';
SELECT product(x), toTypeName(product(x))
FROM VALUES('x Float64', (1.5), (2), (4));

SELECT 'negative values and zero';
SELECT product(x) FROM VALUES('x Int64', (-2), (3));
SELECT product(x) FROM VALUES('x Int64', (5), (0), (7));

SELECT 'empty and nullable input';
SELECT product(number), toTypeName(product(number)) FROM numbers(0);
SELECT toTypeName(productOrNull(number)), productOrNull(number) FROM numbers(0);
SELECT toTypeName(productOrDefault(number)), productOrDefault(number) FROM numbers(0);
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
SELECT isNull(productIf(x, isFinite(x))), productIf(x, isFinite(x))
FROM VALUES('x Float64', (2), (nan), (3), (inf));

SELECT 'filtered non-finite and nullable input';
SELECT productIf(x, keep) = 6
FROM VALUES('x Nullable(Float64), keep UInt8', (2, 1), (NULL, 1), (nan, 0), (inf, 0), (3, 1));

SELECT 'filtered Decimal and integer input';
SELECT productIf(x, keep) = 10
FROM VALUES('x Decimal64(2), keep UInt8', (2.00, 1), (0.00, 0), (5.00, 1));
SELECT productIf(x, keep) = 6
FROM VALUES('x Int64, keep UInt8', (2, 1), (0, 0), (3, 1));

SELECT 'non-finite result semantics';
SELECT isNull(product(x)) FROM VALUES('x Float64', (nan), (2));
SELECT isNull(product(x)) FROM VALUES('x Float64', (0), (inf), (2));
SELECT isNull(product(x)) FROM VALUES('x Float64', (inf), (0), (2));
SELECT isNull(product(x)), isInfinite(product(x)) FROM VALUES('x Float64', (inf), (2));

SELECT 'partial states';
SELECT productMerge(state)
FROM
(
    SELECT productState(x) AS state FROM VALUES('x Float64', (2), (3))
    UNION ALL
    SELECT productState(x) AS state FROM VALUES('x Float64', (4), (5))
);
SELECT productMerge(state)
FROM
(
    SELECT productState(toFloat64(number)) AS state FROM numbers(0)
    UNION ALL
    SELECT productState(x) AS state FROM VALUES('x Float64', (2), (3))
);
SELECT isNull(productMerge(state))
FROM
(
    SELECT productState(x) AS state FROM VALUES('x Float64', (0), (inf))
    UNION ALL
    SELECT productState(x) AS state FROM VALUES('x Float64', (2))
);

SELECT 'ordered input';
SELECT product(x) = 0
FROM
(
    SELECT x
    FROM VALUES('x Float64', (1e200), (1e200), (1e-200), (1e-200))
    ORDER BY x ASC
)
SETTINGS max_threads = 1;

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

SELECT 'batch boundaries';
SELECT isInfinite(product(if(number < 2, 1e200, 1e-200))) FROM numbers(4) SETTINGS max_threads = 1, max_block_size = 1;
SELECT isInfinite(product(if(number < 2, 1e200, 1e-200))) FROM numbers(4) SETTINGS max_threads = 1, max_block_size = 2;
SELECT isInfinite(product(if(number < 2, 1e200, 1e-200))) FROM numbers(4) SETTINGS max_threads = 1, max_block_size = 65536;

SELECT 'decimal values';
SELECT product(x) = 10.0
FROM VALUES('x Decimal64(2)', (1.25), (2.00), (4.00));
SELECT product(x) != toFloat64(toDecimal64('0.1', 1) * toDecimal64('0.2', 1))
FROM VALUES('x Decimal64(1)', (0.1), (0.2));

SELECT 'Float64 input arithmetic';
SELECT product(x) > 0, arrayProduct(groupArray(x)) = 0
FROM VALUES('x UInt64', (9223372036854775808), (2));

SELECT 'distinct with dense input';
SELECT product(DISTINCT x) = 0, productDistinct(x) = 0, productDistinctIf(x, 1) = 0
FROM
(
    SELECT if(number = 0, 1e300, if(number = 2, 2e300, 0.)) AS x
    FROM numbers(200)
    ORDER BY number
)
SETTINGS max_threads = 1;

DROP TABLE IF EXISTS product_sparse;
CREATE TABLE product_sparse
(
    id UInt64,
    x Float64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

INSERT INTO product_sparse
SELECT number, if(number = 0, 1e300, if(number = 2, 2e300, 0.))
FROM numbers(200);

SELECT product(x) FROM product_sparse;
SELECT if(id < 3, 0, 1), product(x) FROM product_sparse GROUP BY if(id < 3, 0, 1) ORDER BY if(id < 3, 0, 1);
SELECT product(x), finalizeAggregation(productState(x)) FROM product_sparse;
SELECT if(id < 3, 0, 1), product(x), finalizeAggregation(productState(x))
FROM product_sparse
GROUP BY if(id < 3, 0, 1)
ORDER BY if(id < 3, 0, 1);

OPTIMIZE TABLE product_sparse FINAL;

SELECT serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'product_sparse' AND column = 'x' AND active;
SELECT 'distinct with sparse input';
SELECT product(DISTINCT x) = 0, productDistinct(x) = 0, productDistinctIf(x, 1) = 0
FROM (SELECT x FROM product_sparse ORDER BY id)
SETTINGS max_threads = 1;
SELECT product(DISTINCT x) = 0, productDistinct(x) = 0
FROM (SELECT x FROM product_sparse ORDER BY id)
SETTINGS max_threads = 1, max_block_size = 7;

SELECT product(x) FROM product_sparse;
SELECT if(id < 3, 0, 1), product(x) FROM product_sparse GROUP BY if(id < 3, 0, 1) ORDER BY if(id < 3, 0, 1);
SELECT product(x), finalizeAggregation(productState(x)) FROM product_sparse;
SELECT if(id < 3, 0, 1), product(x), finalizeAggregation(productState(x))
FROM product_sparse
GROUP BY if(id < 3, 0, 1)
ORDER BY if(id < 3, 0, 1);
SELECT productOrNull(x), productOrDefault(x) FROM product_sparse;
SELECT product(x) FROM product_sparse SETTINGS aggregate_functions_null_for_empty = 1;
SELECT if(id < 3, 0, 1), productOrNull(x), productOrDefault(x)
FROM product_sparse
GROUP BY if(id < 3, 0, 1)
ORDER BY if(id < 3, 0, 1);

-- State inside OrDefault must not move sparse defaults past nondefault values.
SELECT 'composed state with dense input';
SELECT finalizeAggregation(productStateOrDefault(x)), finalizeAggregation(productOrDefaultState(x))
FROM
(
    SELECT if(number IN (0, 2), 1e300, 0.) AS x
    FROM numbers(200)
    ORDER BY number
)
SETTINGS max_threads = 1;

SELECT 'composed state with sparse input';
SELECT finalizeAggregation(productStateOrDefault(x)), finalizeAggregation(productOrDefaultState(x))
FROM (SELECT x FROM product_sparse ORDER BY id)
SETTINGS max_threads = 1;

SELECT 'composed state with sparse groups';
SELECT if(id < 3, 0, 1) AS key, finalizeAggregation(productStateOrDefault(x))
FROM (SELECT id, x FROM product_sparse ORDER BY id)
GROUP BY key
ORDER BY key
SETTINGS max_threads = 1;

SELECT 'composed state with only defaults';
SELECT finalizeAggregation(productStateOrDefault(x))
FROM (SELECT x FROM product_sparse WHERE id > 2 ORDER BY id)
SETTINGS max_threads = 1;

SELECT 'composed state with empty input';
SELECT isNull(finalizeAggregation(productStateOrDefault(x)))
FROM product_sparse WHERE 0;

DROP TABLE product_sparse;
