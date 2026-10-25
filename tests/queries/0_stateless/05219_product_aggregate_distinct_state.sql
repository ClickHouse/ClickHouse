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

-- Distinct must keep the order of combined keys, not just unary values.
SELECT 'If ordered state';
SELECT
    isNull(productIfDistinct(x, keep)),
    isNull(finalizeAggregation(productIfDistinctState(x, keep)))
FROM
(
    SELECT x, keep
    FROM VALUES('k UInt8, x Float64, keep UInt8',
        (1, 1e300, 1), (2, 0, 1), (3, 2e300, 1))
    ORDER BY k
)
SETTINGS max_threads = 1;

-- Tuple has multiple nested aggregate functions, not getNestedFunction().
SELECT 'Tuple ordered state';
SELECT
    isNull(tupleElement(productTupleDistinct(tuple(x)), 1)),
    isNull(tupleElement(finalizeAggregation(productTupleDistinctState(tuple(x))), 1))
FROM
(
    SELECT x
    FROM VALUES('k UInt8, x Float64', (1, 1e300), (2, 2e300), (3, 0))
    ORDER BY k
)
SETTINGS max_threads = 1;

-- Both resampling and ArgMax add an argument to the nested aggregate.
SELECT 'ArgMax ordered state';
SELECT
    isNull(productArgMaxDistinct(x, k)),
    isNull(finalizeAggregation(productArgMaxDistinctState(x, k)))
FROM
(
    SELECT x, k
    FROM VALUES('seq UInt8, x Float64, k UInt8',
        (1, 1e300, 1), (2, 0, 1), (3, 2e300, 1))
    ORDER BY seq
)
SETTINGS max_threads = 1;

SELECT 'Resample ordered state';
SELECT
    toString(productResampleDistinct(0, 1, 1)(x, k))
        = toString(finalizeAggregation(productResampleDistinctState(0, 1, 1)(x, k)))
FROM
(
    SELECT x, k
    FROM VALUES('seq UInt8, x Float64, k UInt8',
        (1, 1e300, 0), (2, 0, 0), (3, 2e300, 0))
    ORDER BY seq
)
SETTINGS max_threads = 1;

SELECT 'If and Tuple persisted states';
DROP TABLE IF EXISTS product_composed_distinct_states;
CREATE TABLE product_composed_distinct_states
(
    id UInt8,
    if_state AggregateFunction(productIfDistinct, Float64, UInt8),
    tuple_state AggregateFunction(productTupleDistinct, Tuple(Float64))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO product_composed_distinct_states
SELECT 1, productIfDistinctState(x, keep), productTupleDistinctState(tuple(tuple_x))
FROM
(
    SELECT
        if(number = 0, 1e300, if(number = 1, 0., 2e300)) AS x,
        toUInt8(1) AS keep,
        if(number = 0, 1e300, if(number = 1, 2e300, 0.)) AS tuple_x
    FROM numbers(3)
    ORDER BY number
)
SETTINGS max_threads = 1;

SELECT
    isNull(finalizeAggregation(if_state)),
    isNull(tupleElement(finalizeAggregation(tuple_state), 1))
FROM product_composed_distinct_states
ORDER BY id;
SELECT
    isNull(productIfDistinctMerge(if_state)),
    isNull(tupleElement(productTupleDistinctMerge(tuple_state), 1))
FROM product_composed_distinct_states;
DROP TABLE product_composed_distinct_states;

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
