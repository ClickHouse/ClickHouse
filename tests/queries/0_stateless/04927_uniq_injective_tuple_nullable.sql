-- Tags: no-old-analyzer
-- The old analyzer rewrites the query before types are resolved and keeps the wrong results.
-- https://github.com/ClickHouse/ClickHouse/issues/114784
-- `uniq*` skips the rows where an argument is NULL. An injective function whose result cannot be Nullable
-- hides that nullability - `tuple(NULL)` and `bitmaskToArray(NULL)` are values that get counted - so
-- `optimize_injective_functions_inside_uniq` must not remove it.

SELECT 'tuple';
SELECT uniq(tuple(x)), uniqExact(tuple(x)), uniqHLL12(tuple(x)), uniqCombined(tuple(x)), uniqCombined64(tuple(x))
FROM values('x Nullable(Int32)', 1, 2, NULL)
SETTINGS optimize_injective_functions_inside_uniq = 1;

SELECT uniq(tuple(x)), uniqExact(tuple(x)), uniqHLL12(tuple(x)), uniqCombined(tuple(x)), uniqCombined64(tuple(x))
FROM values('x Nullable(Int32)', 1, 2, NULL)
SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'nested, LowCardinality and multiple arguments';
SELECT uniqExact(tuple(tuple(x))), uniqExact(tuple(toLowCardinality(x)))
FROM values('x Nullable(Int32)', 1, 2, NULL)
SETTINGS optimize_injective_functions_inside_uniq = 1;

SELECT uniqExact(tuple(tuple(x))), uniqExact(tuple(toLowCardinality(x)))
FROM values('x Nullable(Int32)', 1, 2, NULL)
SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT uniqExact(tuple(x), y) FROM values('x Nullable(Int32), y Nullable(Int32)', (1, 1), (2, NULL), (NULL, 3))
SETTINGS optimize_injective_functions_inside_uniq = 1;

SELECT uniqExact(tuple(x), y) FROM values('x Nullable(Int32), y Nullable(Int32)', (1, 1), (2, NULL), (NULL, 3))
SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'constant folding must not change the result';
SELECT countDistinct(tuple(NULL));
SELECT countDistinct(tuple(arrayJoin([NULL])));
SELECT countDistinct(tuple(arrayJoin(emptyArrayToSingle([]::Array(Nullable(Int32))))));

SELECT 'the optimization still applies to a not Nullable argument';
EXPLAIN QUERY TREE SELECT uniqExact(tuple(number)) FROM numbers(3) SETTINGS optimize_injective_functions_inside_uniq = 1;
