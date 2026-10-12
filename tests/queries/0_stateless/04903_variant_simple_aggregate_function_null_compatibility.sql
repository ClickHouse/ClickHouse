SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;

DROP TABLE IF EXISTS variant_simple_aggregate_function_null_compatibility;
CREATE TABLE variant_simple_aggregate_function_null_compatibility
(
    k UInt8,
    v SimpleAggregateFunction(anyLast, Variant(UInt64, String))
)
ENGINE = AggregatingMergeTree
ORDER BY k;

INSERT INTO variant_simple_aggregate_function_null_compatibility VALUES (1, 1::UInt64);
INSERT INTO variant_simple_aggregate_function_null_compatibility VALUES (1, NULL);

-- SimpleAggregateFunction stores raw values, which a merge re-aggregates. Its existing NULL behavior must not
-- change merely because the table is reconstructed without query settings.
OPTIMIZE TABLE variant_simple_aggregate_function_null_compatibility FINAL;
SELECT v FROM variant_simple_aggregate_function_null_compatibility FINAL;

DROP TABLE variant_simple_aggregate_function_null_compatibility;

CREATE TABLE variant_simple_aggregate_function_null_compatibility
(
    k UInt8,
    v SimpleAggregateFunction(any, Variant(UInt64, String))
)
ENGINE = AggregatingMergeTree
ORDER BY k;

-- The -SimpleState producer must use the same historical behavior as the declared type it creates.
-- Otherwise a newly inserted state could skip the leading NULL while a later table merge preserves it.
INSERT INTO variant_simple_aggregate_function_null_compatibility
SELECT 1, anySimpleState(v)
FROM values('v Variant(UInt64, String)', NULL, 1::UInt64);

OPTIMIZE TABLE variant_simple_aggregate_function_null_compatibility FINAL;
SELECT v FROM variant_simple_aggregate_function_null_compatibility FINAL;

DROP TABLE variant_simple_aggregate_function_null_compatibility;
