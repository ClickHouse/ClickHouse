-- Tags: zookeeper

DROP TABLE IF EXISTS default_table;

CREATE TABLE default_table
(
  id UInt64,
  enum_column Enum8('undefined' = 0, 'fox' = 1, 'index' = 2)
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/test_01135/default_table', '1')
ORDER BY tuple();

INSERT INTO default_table VALUES(1, 'index'), (2, 'fox');

ALTER TABLE default_table MODIFY COLUMN enum_column Enum8('undefined' = 0, 'fox' = 1, 'index' = 2) DEFAULT 'undefined';

INSERT INTO default_table (id) VALUES(3), (4);

DETACH TABLE default_table;

ATTACH TABLE default_table;

SELECT COUNT() from default_table;

ALTER TABLE default_table MODIFY COLUMN enum_column Enum8('undefined' = 0, 'fox' = 1, 'index' = 2) DEFAULT 'fox';

SHOW CREATE TABLE default_table;

DROP TABLE IF EXISTS default_table;
