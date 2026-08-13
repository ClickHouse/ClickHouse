-- The crash path only exists in the new analyzer; the old analyzer rejects this query
-- earlier with NO_COMMON_TYPE, so it cannot exercise the regression.
SET enable_analyzer = 1;

-- A Variant column whose local variant order differs from its global order, with one
-- variant being Map, would feed a non-Map source column into ColumnMap statistics during
-- a sorted merge and abort with "Source column is not Map". The query must not crash.
SELECT * FROM
(
    SELECT n, m FROM
    (
        (SELECT 2 AS n, map('z', 'a') AS m FROM numbers(2))
        EXCEPT ALL
        (SELECT map(toFixedString('z', 1), 'a') AS m, 2 AS n FROM numbers(2))
    )
    UNION ALL
    SELECT toLowCardinality(1) AS n, map('-1', 'b') AS m FROM numbers(2)
)
ORDER BY n
SETTINGS allow_suspicious_types_in_order_by = 1
FORMAT Null;
