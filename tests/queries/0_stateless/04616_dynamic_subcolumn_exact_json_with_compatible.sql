-- Regression test: a `Dynamic` subcolumn request with an exactly matching `JSON` variant must also
-- return rows stored under compatible parameterized `JSON(...)` variants (and vice versa), not only
-- the rows of the exact variant.

SET allow_experimental_dynamic_type = 1;
SET enable_json_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS test_exact_compat;

CREATE TABLE test_exact_compat
(
    id UInt64,
    d Dynamic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_exact_compat VALUES (1, CAST(CAST('{"a":1}' AS JSON) AS Dynamic));
INSERT INTO test_exact_compat VALUES (2, CAST(CAST('{"a":2}' AS JSON(max_dynamic_paths=0)) AS Dynamic));
INSERT INTO test_exact_compat VALUES (3, CAST(CAST('{"a":3}' AS JSON(max_dynamic_paths=1)) AS Dynamic));

SELECT
    id,
    dynamicType(d),
    d.JSON.a.:Int64
FROM test_exact_compat
ORDER BY id
FORMAT TSVRaw;

-- Same, from a compacted part (single part holding all three variants).
OPTIMIZE TABLE test_exact_compat FINAL;

SELECT
    id,
    dynamicType(d),
    d.JSON.a.:Int64
FROM test_exact_compat
ORDER BY id
FORMAT TSVRaw;

-- In-memory path (`DataTypeDynamic::getDynamicSubcolumnData`), no storage involved.
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

DROP TABLE test_exact_compat;
