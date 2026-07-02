-- WITH TOTALS + group_by_use_nulls is only supported by the analyzer.
SET enable_analyzer = 1;

SELECT number
FROM numbers(10)
GROUP BY number
    WITH TOTALS
ORDER BY number
SETTINGS group_by_use_nulls = 1;

-- Constant GROUP BY keys are folded away before aggregation, so they are not
-- real key columns and keep their constant value in the TOTALS row (they are
-- not nullified). This matches CUBE/ROLLUP/GROUPING SETS WITH TOTALS, where a
-- folded constant key likewise stays constant in the summary rows.
SELECT 1 AS k, count()
FROM numbers(10)
GROUP BY k
    WITH TOTALS
SETTINGS group_by_use_nulls = 1;

-- Mixed real and constant keys: the real key is NULL in the TOTALS row, the
-- folded constant key keeps its value.
SELECT number % 2 AS r, 1 AS c, count()
FROM numbers(10)
GROUP BY r, c
    WITH TOTALS
ORDER BY r
SETTINGS group_by_use_nulls = 1;

-- Expression-level coverage: the key must be analyzed as Nullable in the query
-- tree, not only carry a NULL value at runtime. toTypeName reports Nullable for
-- every row and `k IS NULL` is 1 in the TOTALS row (not folded to 0).
SELECT number AS k, toTypeName(k), k IS NULL
FROM numbers(3)
GROUP BY k
    WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1;

-- A wrapping subquery sees the WITH TOTALS key as Nullable, so the TOTALS row
-- can be selected with `WHERE k IS NULL`.
SELECT k, toTypeName(k)
FROM
(
    SELECT number AS k
    FROM numbers(3)
    GROUP BY k
        WITH TOTALS
)
WHERE k IS NULL
SETTINGS group_by_use_nulls = 1;

-- GROUP BY ALL WITH TOTALS must behave exactly like the explicit GROUP BY above:
-- the implicitly-collected key is analyzed as Nullable in the query tree (so
-- toTypeName reports Nullable and `k IS NULL` is 1 in the TOTALS row), not only
-- carrying a NULL value at runtime. GROUP BY ALL keys are expanded after the
-- nullable-key registration normally runs, so this guards that expansion order.
SELECT number AS k, toTypeName(k), k IS NULL
FROM numbers(3)
GROUP BY ALL
    WITH TOTALS
ORDER BY k
SETTINGS group_by_use_nulls = 1;

-- The wrapping subquery resolves the outer `k` against the inner Nullable type,
-- so `WHERE k IS NULL` selects the TOTALS row. With GROUP BY ALL.
SELECT k, toTypeName(k)
FROM
(
    SELECT number AS k
    FROM numbers(3)
    GROUP BY ALL
        WITH TOTALS
)
WHERE k IS NULL
SETTINGS group_by_use_nulls = 1;
