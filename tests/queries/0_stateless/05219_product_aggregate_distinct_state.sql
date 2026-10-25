-- Replaying unordered DISTINCT keys used to turn NULL into 0 after copying
-- or persisting a productDistinct state.
SELECT 'numeric state copy';
SELECT
    isNull(productDistinct(x)),
    isNull(finalizeAggregation(productDistinctState(x))),
    isNull(productDistinctIf(x, 1))
FROM
(
    SELECT x
    FROM VALUES('k UInt8, x Float64', (1, 1e300), (2, 2e300), (3, 1e300), (4, 0))
    ORDER BY k
)
SETTINGS max_threads = 1;

SELECT 'numeric finite state copy';
SELECT productDistinct(x), finalizeAggregation(productDistinctState(x))
FROM VALUES('x Float64', (2), (3), (2), (4));

SELECT 'MergeTree state round trip';
DROP TABLE IF EXISTS product_distinct_state_order;
CREATE TABLE product_distinct_state_order
(
    id UInt8,
    state AggregateFunction(productDistinct, Float64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO product_distinct_state_order
SELECT 1, productDistinctState(x)
FROM
(
    SELECT x
    FROM VALUES('k UInt8, x Float64', (1, 1e300), (2, 2e300), (3, 0))
    ORDER BY k
)
SETTINGS max_threads = 1;

-- Ensure both state finalization and state merging survive persistence.
SELECT isNull(finalizeAggregation(state))
FROM product_distinct_state_order
ORDER BY id;
SELECT isNull(productDistinctMerge(state))
FROM product_distinct_state_order;
DROP TABLE product_distinct_state_order;

-- The ordered state must also be used when product is wrapped in
-- OrNull/OrDefault before Distinct is applied.
SELECT 'wrapped numeric state copy';
SELECT
    isNull(productOrNull(DISTINCT x)),
    isNull(finalizeAggregation(productOrNullDistinctState(x))),
    isNull(productOrDefaultDistinct(x)),
    isNull(finalizeAggregation(productOrDefaultDistinctState(x)))
FROM
(
    SELECT x
    FROM VALUES('k UInt8, x Float64', (1, 1e300), (2, 2e300), (3, 0))
    ORDER BY k
)
SETTINGS max_threads = 1;

SELECT 'wrapped MergeTree state round trip';
DROP TABLE IF EXISTS product_wrapped_distinct_state_order;
CREATE TABLE product_wrapped_distinct_state_order
(
    id UInt8,
    null_state AggregateFunction(productOrNullDistinct, Float64),
    default_state AggregateFunction(productOrDefaultDistinct, Float64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO product_wrapped_distinct_state_order
SELECT 1, productOrNullDistinctState(x), productOrDefaultDistinctState(x)
FROM
(
    SELECT x
    FROM VALUES('k UInt8, x Float64', (1, 1e300), (2, 2e300), (3, 0))
    ORDER BY k
)
SETTINGS max_threads = 1;

SELECT isNull(finalizeAggregation(null_state)), isNull(finalizeAggregation(default_state))
FROM product_wrapped_distinct_state_order
ORDER BY id;
SELECT isNull(productOrNullDistinctMerge(null_state)), isNull(productOrDefaultDistinctMerge(default_state))
FROM product_wrapped_distinct_state_order;
DROP TABLE product_wrapped_distinct_state_order;

-- Decimal256 takes the generic Distinct path. Six different 70-digit values
-- overflow Float64 before the zero, so the ordered result is NULL.
SELECT 'Decimal256 state copy';
SELECT isNull(productDistinct(x)), isNull(finalizeAggregation(productDistinctState(x)))
FROM
(
    SELECT if(number = 6, toDecimal256(0, 0),
        toDecimal256(concat(toString(number + 1), repeat('0', 69)), 0)) AS x
    FROM numbers(7)
    ORDER BY number
)
SETTINGS max_threads = 1;
