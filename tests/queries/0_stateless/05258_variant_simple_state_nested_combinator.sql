-- A `-SimpleState` value keeps the historical Variant NULL behavior of the `SimpleAggregateFunction` type it creates,
-- also when `-SimpleState` is not the outermost combinator.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_variant_types = 1;
SET aggregate_functions_skip_variant_nulls = 1;
SET max_threads = 1;

SELECT 'SimpleState outermost';
SELECT anySimpleState(v) FROM values('v Variant(UInt64, String)', NULL, 'x');

SELECT 'SimpleState under -If';
SELECT anySimpleStateIf(v, 1) FROM values('v Variant(UInt64, String)', NULL, 'x');
SELECT toTypeName(anySimpleStateIf(v, 1)) FROM values('v Variant(UInt64, String)', NULL, 'x');

SELECT 'SimpleState under -ArgMax';
SELECT anySimpleStateArgMax(v, k) FROM values('v Variant(UInt64, String), k UInt8', (NULL, 10), ('x', 5));

SELECT 'SimpleState under -If, stored and merged';
DROP TABLE IF EXISTS variant_simple_state_nested_combinator;
CREATE TABLE variant_simple_state_nested_combinator
(
    k UInt8,
    v SimpleAggregateFunction(any, Variant(UInt64, String))
)
ENGINE = AggregatingMergeTree
ORDER BY k;

INSERT INTO variant_simple_state_nested_combinator
SELECT 1, anySimpleStateIf(v, 1)
FROM values('v Variant(UInt64, String)', NULL, 'x');

INSERT INTO variant_simple_state_nested_combinator VALUES (1, 'y');

OPTIMIZE TABLE variant_simple_state_nested_combinator FINAL;
SELECT v FROM variant_simple_state_nested_combinator FINAL;

DROP TABLE variant_simple_state_nested_combinator;

SELECT 'Plain aggregate functions still skip Variant NULLs';
SELECT anyIf(v, 1) FROM values('v Variant(UInt64, String)', NULL, 'x');
SELECT anyArgMax(v, k) FROM values('v Variant(UInt64, String), k UInt8', (NULL, 10), ('x', 5));
