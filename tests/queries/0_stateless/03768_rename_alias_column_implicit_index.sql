-- Test that renaming ALIAS columns correctly updates implicit minmax indices

DROP TABLE IF EXISTS test_rename_alias;

CREATE TABLE test_rename_alias (
    value Int32,
    alias_col UInt8 ALIAS value > 0
) ENGINE = MergeTree
ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = true;

INSERT INTO test_rename_alias VALUES (1), (-1), (0);

-- Verify implicit index exists for alias_col
SELECT name, type, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'test_rename_alias'
ORDER BY name;

-- Test renaming the ALIAS column
ALTER TABLE test_rename_alias RENAME COLUMN alias_col TO alias_renamed;

-- Verify implicit index was renamed
SELECT name, type, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'test_rename_alias'
ORDER BY name;

-- Verify the renamed column works correctly
SELECT alias_renamed FROM test_rename_alias ORDER BY value;

DROP TABLE test_rename_alias;
