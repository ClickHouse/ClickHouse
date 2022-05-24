DROP TABLE IF EXISTS table_01;

DROP TABLE IF EXISTS table_02;

DROP TABLE IF EXISTS table_03;

CREATE TABLE table_01 (
                          date Date,
                          n Int32
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY date;

CREATE TABLE table_02 (
                          date Date,
                          n Int32
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY date
Settings storage_policy='local1';

CREATE TABLE table_03 (
                          date Date,
                          n Int32
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY date
Settings storage_policy='local2';

INSERT INTO table_01 SELECT toDate('2019-10-01'), number FROM system.numbers LIMIT 1000;

SELECT COUNT() FROM table_01;

ALTER TABLE table_02 ATTACH PARTITION '2019-10-01' FROM table_01;

SELECT COUNT() FROM table_02;

ALTER TABLE table_03 ATTACH PARTITION '2019-10-01' FROM table_02;

SELECT COUNT() FROM table_03;

DROP TABLE IF EXISTS table_01;

DROP TABLE IF EXISTS table_02;

DROP TABLE IF EXISTS table_03;
