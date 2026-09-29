CREATE TABLE sparse_missing_defaults (k UInt64, a UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_subcolumns',
    ratio_of_defaults_for_sparse_serialization = 1, min_bytes_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO sparse_missing_defaults VALUES (1, 0);
ALTER TABLE sparse_missing_defaults MODIFY COLUMN a Nullable(UInt64);
SELECT k, a FROM sparse_missing_defaults;
DROP TABLE sparse_missing_defaults;

-- Primitive columns have no sparse subcolumns even when sparse serialization is enabled.
CREATE TABLE sparse_missing_defaults (k UInt64, a UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_subcolumns',
    ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO sparse_missing_defaults VALUES (1, 0);
ALTER TABLE sparse_missing_defaults MODIFY COLUMN a Nullable(UInt64);
SELECT k, a FROM sparse_missing_defaults;
DROP TABLE sparse_missing_defaults;
