WITH minSimpleState(value) AS c
SELECT toTypeName(c), c
FROM (
    SELECT NULL as value
    UNION ALL
    SELECT 1 as value
);
