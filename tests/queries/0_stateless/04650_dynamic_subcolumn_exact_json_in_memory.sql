-- Tags: no-old-analyzer
-- The old analyzer cannot resolve a subcolumn of a column produced by a `UNION ALL` subquery at all
-- (it fails with `Not found column d.JSON.a.:`Int64` in block`), which is unrelated to what is tested here.

-- Regression test for the in-memory path (`DataTypeDynamic::getDynamicSubcolumnData`): a `Dynamic`
-- subcolumn request with an exactly matching `JSON` variant must also return rows stored under
-- compatible parameterized `JSON(...)` variants. The storage paths are covered by
-- `04616_dynamic_subcolumn_exact_json_with_compatible`.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

SELECT
    id,
    dynamicType(d),
    d.JSON.a.:Int64
FROM
(
    SELECT 1 AS id, CAST(CAST('{"a":1}' AS JSON) AS Dynamic) AS d
    UNION ALL
    SELECT 2 AS id, CAST(CAST('{"a":2}' AS JSON(max_dynamic_paths=0)) AS Dynamic) AS d
)
ORDER BY id
FORMAT TSVRaw;
