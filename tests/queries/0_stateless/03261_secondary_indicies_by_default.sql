set mutations_sync = 2;

DROP TABLE IF EXISTS tbl;

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

DROP TABLE IF EXISTS tbl;