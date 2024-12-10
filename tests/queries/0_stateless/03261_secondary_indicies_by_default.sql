SET mutations_sync = 2;

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;

CREATE TABLE tbl
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

SELECT 'Default';
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Enable enable_minmax_index_for_all_numeric_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = true;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Disable enable_minmax_index_for_all_numeric_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = false;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Enable enable_minmax_index_for_all_numeric_columns again';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = true;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Add numeric column';
ALTER TABLE tbl ADD COLUMN w Int;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Drop numeric column';
ALTER TABLE tbl DROP COLUMN w;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Add string column';
ALTER TABLE tbl ADD COLUMN s String;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Enable enable_minmax_index_for_all_string_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_string_columns = true;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Add string column';
ALTER TABLE tbl ADD COLUMN t String;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Drop string column';
ALTER TABLE tbl DROP COLUMN t;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Rename column s to ss';
ALTER TABLE tbl RENAME COLUMN s to ss;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

SELECT 'Disable enable_minmax_index_for_all_string_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_string_columns = false;
SELECT name,type,expr FROM system.data_skipping_indices WHERE table = 'tbl';

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

ALTER TABLE tbl2 ADD INDEX _idx_minmax_n_y y TYPE minmax GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS tbl;
DROP TABLE IF EXISTS tbl2;