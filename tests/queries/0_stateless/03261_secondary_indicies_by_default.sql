SET mutations_sync = 2;

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;
DROP TABLE IF EXISTS tbl3;

CREATE TABLE tbl
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key
SETTINGS enable_minmax_index_for_all_numeric_columns = true, enable_minmax_index_for_all_string_columns = true;

INSERT INTO tbl VALUES (1,1,1),(2,2,2),(3,3,3);

SELECT 'tbl with enable_minmax_index_for_all_numeric_columns and enable_minmax_index_for_all_string_columns enabled';
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = false; -- { serverError READONLY_SETTING }
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_string_columns = false; -- { serverError READONLY_SETTING }

SELECT 'Add numeric column';
ALTER TABLE tbl ADD COLUMN w Int;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

SELECT 'After materialize';
ALTER TABLE tbl MATERIALIZE INDEX _idx_minmax_n_w;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

SELECT 'Drop numeric column';
ALTER TABLE tbl DROP COLUMN w;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

SELECT 'Add string column';
ALTER TABLE tbl ADD COLUMN s String;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

SELECT 'Add another string column';
ALTER TABLE tbl ADD COLUMN t String;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

SELECT 'Drop string column t';
ALTER TABLE tbl DROP COLUMN t;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

SELECT 'Rename column s to ss';
ALTER TABLE tbl RENAME COLUMN s to ss;
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl' and database = currentDatabase();

CREATE TABLE tbl2
(
    key Int,
    x Int,
    y Int,
    INDEX _idx_minmax_n_x x TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tbl2
(
    key Int,
    x Int,
    y Int
)
Engine=MergeTree()
ORDER BY key;

ALTER TABLE tbl2 ADD INDEX _idx_minmax_s_y y TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tbl3
(
    key Int,
    x Int,
    y Int,
    s String,
    INDEX x_idx x TYPE minmax GRANULARITY 1
)
    Engine=MergeTree()
ORDER BY key;

SELECT 'tbl3 with enable_minmax_index_for_all_numeric_columns and enable_minmax_index_for_all_string_columns disabled';
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl3' and database = currentDatabase();

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;
DROP TABLE IF EXISTS tbl3;