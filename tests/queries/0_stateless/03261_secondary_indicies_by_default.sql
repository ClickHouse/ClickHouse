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

SHOW INDICES FROM tbl;

ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = true;
SHOW INDICES FROM tbl;

ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = false;
SHOW INDICES FROM tbl;

ALTER TABLE tbl MODIFY SETTING enable_minmax_index_for_all_numeric_columns = true;
SHOW INDICES FROM tbl;

ALTER TABLE tbl ADD COLUMN w Int;
SHOW INDICES FROM tbl;

ALTER TABLE tbl DROP COLUMN w;
SHOW INDICES FROM tbl;

DROP TABLE IF EXISTS tbl;