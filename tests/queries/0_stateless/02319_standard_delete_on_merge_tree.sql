DROP TABLE IF EXISTS merge_table_standard_delete;

CREATE TABLE merge_table_standard_delete(id Int32, name String) ENGINE = MergeTree order by id;

INSERT INTO merge_table_standard_delete select number, toString(number) from numbers(100);

SET mutations_sync = 1;

DELETE FROM merge_table_standard_delete WHERE id = 10;

SELECT COUNT() FROM merge_table_standard_delete;

DELETE FROM merge_table_standard_delete WHERE name IN ('1','2','3','4');

SELECT COUNT() FROM merge_table_standard_delete;

DELETE FROM merge_table_standard_delete WHERE 1;

SELECT COUNT() FROM merge_table_standard_delete;

DROP TABLE merge_table_standard_delete;
