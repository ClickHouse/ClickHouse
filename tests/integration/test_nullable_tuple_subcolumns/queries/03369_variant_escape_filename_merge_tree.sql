-- Tuple-related queries from tests/queries/0_stateless/03369_variant_escape_filename_merge_tree.sql.

SET enable_variant_type = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    v Variant(Tuple(a UInt32, b UInt32))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    escape_variant_subcolumn_filenames = 1,
    replace_long_file_name_to_hash = 0;
INSERT INTO test SELECT tuple(1, 2)::Tuple(a UInt32, b UInt32);
SELECT filenames FROM system.parts_columns WHERE table = 'test' AND database = currentDatabase();
DROP TABLE test;

CREATE TABLE test
(
    v Variant(Tuple(a UInt32, b UInt32))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    escape_variant_subcolumn_filenames = 0,
    replace_long_file_name_to_hash = 0;
INSERT INTO test SELECT tuple(1, 2)::Tuple(a UInt32, b UInt32);
SELECT filenames FROM system.parts_columns WHERE table = 'test' AND database = currentDatabase();
DROP TABLE test;
