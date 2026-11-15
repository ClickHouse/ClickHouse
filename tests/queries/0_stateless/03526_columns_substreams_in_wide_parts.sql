DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    int UInt32,
    str String,
    t Tuple(
        a UInt32,
        b Array(UInt32)),
    json JSON(a UInt32, b Array(String)),
    nested Nested(a UInt32, b UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, vertical_merge_algorithm_min_rows_to_activate=1000000, vertical_merge_algorithm_min_columns_to_activate=100000, object_serialization_version='v2', enable_block_number_column=0, enable_block_offset_column=0, replace_long_file_name_to_hash=0, escape_variant_subcolumn_filenames=0, serialization_info_version='basic';

INSERT INTO test SELECT 42, 'str', tuple(42, [1, 2, 3]), '{"a" : 42, "b" : ["a", "b", "c"], "d" : "Hello", "e" : 42, "f" : [{"g" : 42, "k" : [1, 2, 3]}]}', [1, 2, 3], [1, 2, 3];
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

INSERT INTO test SELECT 42, 'str', tuple(42, [1, 2, 3]), '{"a" : 42, "b" : ["a", "b", "c"], "d" : "Hello", "e" : 42, "f" : [{"g" : 42, "k" : [1, 2, 3]}]}', [1, 2, 3], [1, 2, 3];
OPTIMIZE TABLE test FINAL;
SELECT 'Horizontal merge';
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

ALTER TABLE test MODIFY SETTING vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1;
INSERT INTO test SELECT 42, 'str', tuple(42, [1, 2, 3]), '{"a" : 42, "b" : ["a", "b", "c"], "d" : "Hello", "e" : 42, "f" : [{"g" : 42, "k" : [1, 2, 3]}]}', [1, 2, 3], [1, 2, 3];
OPTIMIZE TABLE test FINAL;
SELECT 'Vertical merge';
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter add column';
ALTER TABLE test ADD COLUMN x Array(UInt32);
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';
SELECT 'After merge';
OPTIMIZE TABLE test FINAL;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter drop column';
ALTER TABLE test DROP COLUMN int;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter rename column';
ALTER TABLE test RENAME COLUMN t TO tt;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter drop and rename column';
ALTER TABLE test DROP COLUMN str, RENAME COLUMN x TO str;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter modify column';
ALTER TABLE test MODIFY COLUMN tt Tuple(a UInt32, b Array(String), c UInt32);
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter update column';
ALTER TABLE test UPDATE tt = tuple(42, ['a'], 42) WHERE 1;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter rename nested column';
ALTER TABLE test RENAME COLUMN nested.a to nested.aa;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

SELECT 'Alter rename all nested column';
ALTER TABLE test RENAME COLUMN nested.aa to nested.aaa, RENAME COLUMN nested.b to nested.bbb;
SELECT column, type, substreams, filenames FROM system.parts_columns where database=currentDatabase() and table = 'test' and active;
SELECT '-------------------------------------------------------------------------';

DROP TABLE test;

