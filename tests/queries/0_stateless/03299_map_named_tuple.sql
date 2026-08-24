
-- This is a very subtle issue when map(1 ,2) data type (which is nested with Array(Tuple(key, value))) firstly created with
-- explicitly named tuple ('keys' key, 'values' value) as an argument of `mapConcat`, and then as a constant is referenced by
-- map constant in the lambda function of mapApply which creates its type without explicit naming and further these types
-- are compared, and because tuple naming is participating in this comparison the result is negative - which leads to
-- 'incompatible types' error showing exactly same Map types - issue https://github.com/ClickHouse/ClickHouse/issues/64805

SELECT mapConcat(map(1, 2)), mapApply((x, y) -> (map(1, 2), x + 1), map(1, 0))
