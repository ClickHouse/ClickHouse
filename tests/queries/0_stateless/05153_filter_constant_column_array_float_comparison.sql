DROP TABLE IF EXISTS array_float_comparison;

-- `Array(Int64)` has no least supertype with `Array(Float64)`, so the arrays are compared element-wise with the
-- accurate scalar comparison: an `Int64` that is not exactly representable as a `Float64` is not equal to the
-- rounded `Float64`. That accuracy is what makes the constant substitution below sound - a passing row has
-- exactly one possible stored value.
SELECT [toInt64(9007199254740993)] = [toFloat64(9007199254740992)];
SELECT tuple([toInt64(9007199254740993)]) = tuple([toFloat64(9007199254740992)]);

CREATE TABLE array_float_comparison (a Array(Int32), t Tuple(Array(Int32))) ENGINE = Memory;
INSERT INTO array_float_comparison VALUES
    ([16777217], tuple([16777217])),
    ([16777216], tuple([16777216]));

SET query_plan_merge_filters = 0,
    query_plan_optimize_lazy_materialization = 0,
    query_plan_remove_unused_columns = 0;

SELECT 'array enabled', a, dumpColumnStructure(a) LIKE '%Const%'
FROM array_float_comparison
WHERE a = [toFloat64(16777216)]
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'array disabled', a, dumpColumnStructure(a) LIKE '%Const%'
FROM array_float_comparison
WHERE a = [toFloat64(16777216)]
SETTINGS optimize_constant_columns_after_filter = 0;

SELECT 'tuple enabled', t, dumpColumnStructure(t) LIKE '%Const%'
FROM array_float_comparison
WHERE t = tuple([toFloat64(16777216)])
SETTINGS optimize_constant_columns_after_filter = 1;

SELECT 'tuple disabled', t, dumpColumnStructure(t) LIKE '%Const%'
FROM array_float_comparison
WHERE t = tuple([toFloat64(16777216)])
SETTINGS optimize_constant_columns_after_filter = 0;

DROP TABLE array_float_comparison;
