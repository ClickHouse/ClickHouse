SET mutations_sync = 2;

DROP TABLE IF EXISTS tbl1;
DROP TABLE IF EXISTS tbl2;
DROP TABLE IF EXISTS tbl3;

CREATE TABLE tbl1
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax
)
ENGINE=MergeTree()
ORDER BY key
SETTINGS enable_minmax_index_for_all_numeric_columns = true, enable_minmax_index_for_all_string_columns = true;

INSERT INTO tbl1 VALUES (1,1,1), (2,2,2), (3,3,3);

SELECT 'tbl1 with enable_minmax_index_for_all_numeric_columns and enable_minmax_index_for_all_string_columns enabled';
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

ALTER TABLE tbl1 MODIFY SETTING enable_minmax_index_for_all_numeric_columns = false; -- { serverError READONLY_SETTING }
ALTER TABLE tbl1 MODIFY SETTING enable_minmax_index_for_all_string_columns = false; -- { serverError READONLY_SETTING }
ALTER TABLE tbl1 RESET SETTING enable_minmax_index_for_all_numeric_columns; -- { serverError READONLY_SETTING }
ALTER TABLE tbl1 RESET SETTING enable_minmax_index_for_all_string_columns; -- { serverError READONLY_SETTING }

SELECT 'Add numeric column';
ALTER TABLE tbl1 ADD COLUMN w Int;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'After materialize';
ALTER TABLE tbl1 MATERIALIZE INDEX _idx_minmax_n_w;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'Drop numeric column';
ALTER TABLE tbl1 DROP COLUMN w;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'Add string column';
ALTER TABLE tbl1 ADD COLUMN s String;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'Add another string column';
ALTER TABLE tbl1 ADD COLUMN t String;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'Drop string column t';
ALTER TABLE tbl1 DROP COLUMN t;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'Rename column s to ss';
ALTER TABLE tbl1 RENAME COLUMN s to ss;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

CREATE TABLE tbl2
(
    key Int,
    x Int,
    y Int,
    INDEX _idx_minmax_n_x x TYPE minmax
)
ENGINE=MergeTree()
ORDER BY key; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tbl2
(
    key Int,
    x Int,
    y Int
)
ENGINE=MergeTree()
ORDER BY key;

ALTER TABLE tbl2 ADD INDEX _idx_minmax_s_y y TYPE minmax; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tbl3
(
    key Int,
    x Int,
    y Int,
    s String,
    INDEX x_idx x TYPE minmax
)
ENGINE=MergeTree()
ORDER BY key;

SELECT 'tbl3 with enable_minmax_index_for_all_numeric_columns and enable_minmax_index_for_all_string_columns disabled';
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl3' AND database = currentDatabase();

DROP TABLE tbl1;
DROP TABLE tbl2;
DROP TABLE tbl3;
