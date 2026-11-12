-- https://github.com/ClickHouse/ClickHouse/issues/83433
-- `GROUP BY ALL` used to add the SELECT tuple as a single grouping key without unwrapping it into
-- its elements, unlike an explicit `GROUP BY tuple(...)`. Combined with `OrderByTupleEliminationPass`
-- (which rewrites `ORDER BY tuple(a, b)` into `ORDER BY a, b`), this referenced columns missing from
-- the aggregated block and threw NOT_FOUND_COLUMN_IN_BLOCK.

SET enable_analyzer = 1;

SELECT ((v0.c0, v0.c1)) FROM (SELECT 1 c0, 2 c1) v0 GROUP BY ALL ORDER BY ALL
    SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT ((v0.c0, v0.c1)) FROM (SELECT 1 c0, 2 c1) v0 GROUP BY ALL ORDER BY ALL
    SETTINGS optimize_injective_functions_in_group_by = 1;

-- `GROUP BY ALL` with an explicit `ORDER BY` of the same tuple.
SELECT tuple(c0, c1) AS t FROM (SELECT 1 c0, 2 c1) v0 GROUP BY ALL ORDER BY t
    SETTINGS optimize_injective_functions_in_group_by = 0;

-- Multiple rows, to check the grouping is still correct after unwrapping the tuple key.
SELECT (c0, c1) FROM (SELECT number % 3 AS c0, number % 2 AS c1 FROM numbers(10)) GROUP BY ALL ORDER BY ALL
    SETTINGS optimize_injective_functions_in_group_by = 0;

-- Nested tuple: only the outermost tuple key is unwrapped, mirroring explicit `GROUP BY`.
SELECT (c0, (c0, c1)) FROM (SELECT 1 c0, 2 c1) v0 GROUP BY ALL ORDER BY ALL
    SETTINGS optimize_injective_functions_in_group_by = 0;

-- Tuple mixing a plain column with an aggregate: the aggregate is not part of the grouping key.
SELECT (c0, sum(c1)) FROM (SELECT number % 3 AS c0, number AS c1 FROM numbers(10)) GROUP BY ALL ORDER BY ALL
    SETTINGS optimize_injective_functions_in_group_by = 0;
