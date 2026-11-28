-- Expressions equal to GROUP BY keys are matched by exact comparison (including types)
-- when group_by_use_nulls = 1. Previously the comparison ignored types, so constants
-- with equal values but different types or source expressions could be conflated.

SET group_by_use_nulls = 1;

-- A GROUP BY key that contains other keys as subexpressions.
SELECT (number + 1) * 2 AS e, toTypeName(e), count()
FROM numbers(4)
GROUP BY number, number + 1, (number + 1) * 2 WITH ROLLUP
ORDER BY ALL;

-- A constant key inside a lambda: the projection must read the per-grouping-set
-- key column instead of being folded into a constant.
SELECT DISTINCT 1 AS a, arrayMap(x -> intDiv(x, a), [1, 2, 3]) AS m
GROUP BY a, m WITH CUBE
ORDER BY ALL
SETTINGS max_threads = 1;

-- Constant keys with equal values but different types must not be conflated.
SELECT DISTINCT toUInt8(1) AS a, toUInt16(1) AS b, toTypeName(a), toTypeName(b)
GROUP BY a, b WITH CUBE
ORDER BY ALL
SETTINGS max_threads = 1;

-- A key under a lambda when the whole expression is a key.
SELECT arrayMap(x -> x + number, [1, 2]) AS m, toTypeName(m), count()
FROM numbers(2)
GROUP BY number, arrayMap(x -> x + number, [1, 2]) WITH ROLLUP
ORDER BY ALL;

-- Reuse of a converted expression through an alias.
SELECT number + 1 AS x, x AS y, toTypeName(y), count()
FROM numbers(3)
GROUP BY number, number + 1 WITH ROLLUP
ORDER BY ALL;

-- A constant GROUP BY key captured inside a lambda. After aggregation the key constant
-- is wrapped into Nullable but keeps its name, so the recomputed capture must not reuse it.
SELECT 1 AS a, arrayMap(x -> intDiv(x, a), [1, 2, 3]) AS m
GROUP BY a, m WITH CUBE
ORDER BY ALL
SETTINGS max_threads = 1;

SELECT 1 AS a, arrayMap(x -> intDiv(x, 1), [1, 2, 3]) AS m
GROUP BY a, m WITH CUBE
ORDER BY ALL
SETTINGS max_threads = 1;

-- A key that passes another key to a lambda function: the lambda argument type is derived
-- from the converted (Nullable) key, so the projection must still read the key.
SELECT x, arrayExists(a -> a > 0, [x]) AS e, toTypeName(e), count()
FROM (SELECT number % 2 AS x FROM numbers(4))
GROUP BY ROLLUP(e, x)
ORDER BY ALL;

SELECT x, y, arrayCount(a -> a > 0, [x, y]) AS c, toTypeName(c), count()
FROM (SELECT number % 2 AS x, intDiv(number, 2) % 2 AS y FROM numbers(4))
GROUP BY ROLLUP(c, x, y)
ORDER BY ALL;
