DROP TABLE IF EXISTS array_float_comparison;

SELECT [toInt64(9007199254740993)] = [toFloat64(9007199254740992)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT tuple([toInt64(9007199254740993)]) = tuple([toFloat64(9007199254740992)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

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
