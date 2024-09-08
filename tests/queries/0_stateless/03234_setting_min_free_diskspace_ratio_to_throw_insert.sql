DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
  id UInt64,
  Data String
) ENGINE = MergeTree() ORDER BY id SETTINGS min_free_diskspace_ratio_to_throw_insert = 1; -- inserts should not go through

INSERT INTO test_table VALUES (1, '1'); -- { serverError 243 }

INSERT INTO test_table settings min_free_diskspace_ratio_to_throw_insert = 0 VALUES (2, '2'); -- works

ALTER TABLE test_table MODIFY SETTING min_free_diskspace_ratio_to_throw_insert = 0;

INSERT INTO test_table VALUES (3, '3'); -- works