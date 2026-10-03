-- Tags: no-old-analyzer
-- The old analyzer cannot resolve a subcolumn of a column produced by a `UNION ALL` subquery at all
-- (it fails with `Not found column d.JSON.a in block`), which is unrelated to what is tested here.

-- Regression test for the in-memory path (`DataTypeDynamic::getDynamicSubcolumnData`): a request such
-- as `d.JSON.a` must also return rows stored under declared-path variants such as `JSON(a UInt64)` or
-- `JSON(a UInt64, b String)`. The storage paths are covered by
-- `04636_dynamic_subcolumn_declared_path_json_variants`.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

SELECT id, dynamicType(d), d.JSON.a, d.JSON.a.:Int64, d.JSON.b
FROM
(
    SELECT 1 AS id, CAST(CAST('{"a":1}' AS JSON) AS Dynamic) AS d
    UNION ALL
    SELECT 2 AS id, CAST(CAST('{"a":2}' AS JSON(a UInt64)) AS Dynamic) AS d
    UNION ALL
    SELECT 3 AS id, CAST(CAST('{"a":3,"b":"x"}' AS JSON(a UInt64, b String)) AS Dynamic) AS d
    UNION ALL
    SELECT 4 AS id, CAST(42 AS Dynamic) AS d
)
ORDER BY id
FORMAT TSVRaw;
