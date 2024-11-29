set mutations_sync = 2;

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
SHOW INDICES FROM tbl;

SELECT 'Enable enable_minmax_index_for_all_numeric_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = true;
SHOW INDICES FROM tbl;

SELECT 'Disable enable_minmax_index_for_all_numeric_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = false;
SHOW INDICES FROM tbl;

SELECT 'Enable enable_minmax_index_for_all_numeric_columns again';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = true;
SHOW INDICES FROM tbl;

SELECT 'Add numeric column';
ALTER TABLE tbl ADD COLUMN w Int;
SHOW INDICES FROM tbl;

SELECT 'Drop numeric column';
ALTER TABLE tbl DROP COLUMN w;
SHOW INDICES FROM tbl;

SELECT 'Add string column';
ALTER TABLE tbl ADD COLUMN s String;
SHOW INDICES FROM tbl;

SELECT 'Enable enable_minmax_index_for_all_string_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_string_columns = true;
SHOW INDICES FROM tbl;

SELECT 'Add string column';
ALTER TABLE tbl ADD COLUMN t String;
SHOW INDICES FROM tbl;

SELECT 'Drop string column';
ALTER TABLE tbl DROP COLUMN t;
SHOW INDICES FROM tbl;

SELECT 'Rename column s to ss';
ALTER TABLE tbl RENAME COLUMN s to ss;
SHOW INDICES FROM tbl;

SELECT 'Disable enable_minmax_index_for_all_string_columns';
ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_string_columns = false;
SHOW INDICES FROM tbl;

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