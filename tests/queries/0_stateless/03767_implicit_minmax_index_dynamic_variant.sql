-- Test that implicit minmax indices correctly skip ALIAS columns
-- with Dynamic underlying type (JSON subcolumns)

DROP TABLE IF EXISTS test_implicit_minmax;

-- Test with JSON ALIAS columns (Dynamic subcolumns should be skipped)
-- and regular columns to verify the feature still works
CREATE TABLE test_implicit_minmax (
    json_col JSON,
    regular_col UInt64,
    alias_json UInt64 ALIAS json_col.field
) ENGINE = MergeTree
ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = true;

-- Verify implicit index created only for regular_col, not for alias_json
SELECT name, type, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'test_implicit_minmax'
ORDER BY name;

-- Verify table works correctly
INSERT INTO test_implicit_minmax (json_col, regular_col)
VALUES ('{"field": 42}', 100);

SELECT alias_json, regular_col FROM test_implicit_minmax;

DROP TABLE test_implicit_minmax;
