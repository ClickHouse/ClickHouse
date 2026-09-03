-- Regression test: casting Array(Tuple(Array(...), ...)) to Array(Tuple(QBit(...), ...))
-- when inner elements go through a Nullable conversion should not cause an exception.
-- Previously, nullable_source was incorrectly propagated through Tuple element wrappers
-- into the Array-to-QBit conversion, replacing the properly converted array column
-- with an unrelated column from the outer Nullable wrapper.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=58b9b010eb7d1210b0c0ec88118ab153a9db0f4d&name_0=MasterCI&name_1=BuzzHouse%20%28arm_asan%29

SET allow_suspicious_variant_types = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_nullable_tuple_type = 1;
SET allow_experimental_bfloat16_type = 1;

-- Basic Array-to-QBit inside a Tuple with Nullable wrapping
SELECT CAST(
    [([1.0, 2.0, 3.0], 'hello')::Nullable(Tuple(Array(Float64), String))],
    'Array(Nullable(Tuple(QBit(BFloat16, 3), String)))'
);

-- With Variant source inside Array (matches the original crash scenario)
SELECT CAST(
    ([1, 2, 3]::Array(Variant(Float64, Int32, Int64, UInt32, UInt64)), 'test')::Nullable(Tuple(Array(Variant(Float64, Int32, Int64, UInt32, UInt64)), String)),
    'Nullable(Tuple(QBit(BFloat16, 3), String))'
);

-- Non-nullable Tuple with Array(Variant) to QBit conversion
SELECT CAST(
    ([1.5, 2.5]::Array(Float64), 'abc'),
    'Tuple(QBit(Float64, 2), String)'
);

-- NULL element inside Array should return NULL, not throw SIZES_OF_ARRAYS_DONT_MATCH
SELECT CAST(
    [NULL::Nullable(Tuple(Array(Float64), String))],
    'Array(Nullable(Tuple(QBit(BFloat16, 3), String)))'
);

-- Mix of non-NULL and NULL rows
SELECT
    CAST(v, 'Array(Nullable(Tuple(QBit(BFloat16, 3), String)))') AS converted
FROM VALUES(
    'v Array(Nullable(Tuple(Array(Float64), String)))',
    [([1.0, 2.0, 3.0], 'hello')],
    [NULL]
);
