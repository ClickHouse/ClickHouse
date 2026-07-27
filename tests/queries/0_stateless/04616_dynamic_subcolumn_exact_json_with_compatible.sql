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
SETTINGS
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 1,
    -- Pin serialization versions: the randomized-settings runs can pick combinations under which
    -- the stored variant type of row 3 is rendered as plain `JSON`, changing `dynamicType` output.
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    dynamic_serialization_version = 'v3';

INSERT INTO test_exact_compat VALUES (1, CAST(CAST('{"a":1}' AS JSON) AS Dynamic));
INSERT INTO test_exact_compat VALUES (2, CAST(CAST('{"a":2}' AS JSON(max_dynamic_paths=0)) AS Dynamic));
INSERT INTO test_exact_compat VALUES (3, CAST(CAST('{"a":3}' AS JSON(max_dynamic_paths=1)) AS Dynamic));

-- The regression assertion is that all three values are visible through `d.JSON.a`; the exact
-- rendering of the stored variant type is not asserted here, as it can legitimately vary with
-- storage serialization details.
SELECT
    id,
    d.JSON.a.:Int64
FROM test_exact_compat
ORDER BY id
FORMAT TSVRaw;

-- Same, from a compacted part (single part holding all three variants).
OPTIMIZE TABLE test_exact_compat FINAL;

SELECT
    id,
    d.JSON.a.:Int64
FROM test_exact_compat
ORDER BY id
FORMAT TSVRaw;

-- The in-memory path (`DataTypeDynamic::getDynamicSubcolumnData`) is covered by
-- `04650_dynamic_subcolumn_exact_json_in_memory` (it needs the analyzer).

DROP TABLE test_exact_compat;
