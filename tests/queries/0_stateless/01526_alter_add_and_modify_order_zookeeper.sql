-- Tags: zookeeper

DROP TABLE IF EXISTS table_for_alter;

SET replication_alter_partitions_sync = 2;

CREATE TABLE table_for_alter
(
    `d` Date,
    `a` String,
    `b` UInt8,
    `x` String,
    `y` Int8,
    `version` UInt64,
    `sign` Int8 DEFAULT 1
)
ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/tables/{database}/01526_alter_add/t1', '1', sign, version)
PARTITION BY y
ORDER BY d
SETTINGS index_granularity = 8192;

INSERT INTO table_for_alter VALUES(toDate('2019-10-01'), 'a', 1, 'aa', 1, 1, 1);

DETACH TABLE table_for_alter;

ATTACH TABLE table_for_alter;


SELECT * FROM table_for_alter;

ALTER TABLE table_for_alter ADD COLUMN order UInt32, MODIFY ORDER BY (d, order);


DETACH TABLE table_for_alter;

ATTACH TABLE table_for_alter;

SELECT * FROM table_for_alter;

SHOW CREATE TABLE table_for_alter;

ALTER TABLE table_for_alter ADD COLUMN datum UInt32, MODIFY ORDER BY (d, order, datum);

INSERT INTO table_for_alter VALUES(toDate('2019-10-02'), 'b', 2, 'bb', 2, 2, 2, 1, 2);

SELECT * FROM table_for_alter ORDER BY d;

SHOW CREATE TABLE table_for_alter;

DETACH TABLE table_for_alter;

ATTACH TABLE table_for_alter;

DROP TABLE IF EXISTS table_for_alter;
