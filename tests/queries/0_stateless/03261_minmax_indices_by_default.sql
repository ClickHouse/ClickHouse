SET mutations_sync = 2;

DROP TABLE IF EXISTS tbl1;
DROP TABLE IF EXISTS tbl2;
DROP TABLE IF EXISTS tbl3;
DROP TABLE IF EXISTS tbl4;
DROP TABLE IF EXISTS tbl5;

CREATE TABLE tbl1
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1
)
ENGINE=MergeTree()
ORDER BY key
SETTINGS add_minmax_index_for_numeric_columns = true,
         add_minmax_index_for_string_columns = true,
         index_granularity = 8192,
         index_granularity_bytes = 10485760;

INSERT INTO tbl1 VALUES (1,1,1), (2,2,2), (3,3,3);

SELECT 'Check skipping indices after table creation';
-- Expect x_idx and two implicit minmax indices
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

-- Settings 'add_minmax_index_for_numeric_columns' and 'add_minmax_index_for_string_columns' cannot be changed after table creation
ALTER TABLE tbl1 MODIFY SETTING add_minmax_index_for_numeric_columns = false; -- { serverError READONLY_SETTING }
ALTER TABLE tbl1 MODIFY SETTING add_minmax_index_for_string_columns = false; -- { serverError READONLY_SETTING }
ALTER TABLE tbl1 RESET SETTING add_minmax_index_for_numeric_columns; -- { serverError READONLY_SETTING }
ALTER TABLE tbl1 RESET SETTING add_minmax_index_for_string_columns; -- { serverError READONLY_SETTING }

SELECT 'Add numeric column';
ALTER TABLE tbl1 ADD COLUMN n Int;
-- the implicitly minmax index is only created but not materialized
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'After materialize';
ALTER TABLE tbl1 MATERIALIZE INDEX auto_minmax_index_n;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

SELECT 'Drop numeric column';
ALTER TABLE tbl1 DROP COLUMN n;
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

SELECT 'Rename column s to t';
ALTER TABLE tbl1 RENAME COLUMN s to t;
SELECT name, type, expr, data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl1' AND database = currentDatabase();

-- Check that users cannot create explicit minmax indices with the names of internal minmax indices

CREATE TABLE tbl2
(
    key Int,
    x Int,
    y Int,
    INDEX auto_minmax_index_x x TYPE minmax
)
ENGINE=MergeTree()
ORDER BY key
SETTINGS add_minmax_index_for_numeric_columns = true; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tbl2
(
    key Int,
    x Int,
    y Int,
    INDEX auto_minmax_index_x x TYPE minmax -- fine, add_minmax_index_for_numeric_columns isn't set
)
ENGINE=MergeTree()
ORDER BY key;

CREATE TABLE tbl3
(
    key Int,
    x Int,
    y Int
)
ENGINE=MergeTree()
ORDER BY key;

ALTER TABLE tbl3 ADD INDEX auto_minmax_index_y y TYPE minmax;

CREATE TABLE tbl4
(
    key Int,
    x Int,
    y Int
)
ENGINE=MergeTree()
ORDER BY key
SETTINGS add_minmax_index_for_string_columns = true;

ALTER TABLE tbl4 ADD INDEX auto_minmax_index_y y TYPE minmax; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tbl5
(
    key Int,
    x Int,
    y Int,
    s String,
    INDEX x_idx x TYPE minmax
)
ENGINE=MergeTree()
ORDER BY key;

SELECT 'tbl5 with add_minmax_index_for_numeric_columns and add_minmax_index_for_string_columns disabled';
SELECT name,type,expr,data_compressed_bytes FROM system.data_skipping_indices WHERE table = 'tbl5' AND database = currentDatabase();

-- check that ATTACH of such tables will not throw "uses a reserved index name" error
DETACH TABLE tbl1;
ATTACH TABLE tbl1;

DROP TABLE tbl1;
DROP TABLE tbl2;
DROP TABLE tbl3;
DROP TABLE tbl4;
DROP TABLE tbl5;
