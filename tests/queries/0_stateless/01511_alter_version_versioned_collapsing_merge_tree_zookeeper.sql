DROP TABLE IF EXISTS table_with_version_replicated_1;
DROP TABLE IF EXISTS table_with_version_replicated_2;

CREATE TABLE table_with_version_replicated_1
(
    key UInt64,
    value String,
    version UInt8,
    sign Int8
)
ENGINE ReplicatedVersionedCollapsingMergeTree('/clickhouse/test_01511/t', '1', sign, version)
ORDER BY key;

CREATE TABLE table_with_version_replicated_2
(
    key UInt64,
    value String,
    version UInt8,
    sign Int8
)
ENGINE ReplicatedVersionedCollapsingMergeTree('/clickhouse/test_01511/t', '2', sign, version)
ORDER BY key;

INSERT INTO table_with_version_replicated_1 VALUES (1, '1', 1, -1);
INSERT INTO table_with_version_replicated_1 VALUES (2, '2', 2, -1);

SELECT * FROM table_with_version_replicated_1 ORDER BY key;

SHOW CREATE TABLE table_with_version_replicated_1;

ALTER TABLE table_with_version_replicated_1 MODIFY COLUMN version UInt32 SETTINGS replication_alter_partitions_sync=2;

SELECT * FROM table_with_version_replicated_1 ORDER BY key;

SHOW CREATE TABLE table_with_version_replicated_1;

INSERT INTO TABLE table_with_version_replicated_1 VALUES(1, '1', 1, 1);
INSERT INTO TABLE table_with_version_replicated_1 VALUES(1, '1', 2, 1);

SELECT * FROM table_with_version_replicated_1 FINAL ORDER BY key;

INSERT INTO TABLE table_with_version_replicated_1 VALUES(3, '3', 65555, 1);

SELECT * FROM table_with_version_replicated_1 FINAL ORDER BY key;

INSERT INTO TABLE table_with_version_replicated_1 VALUES(3, '3', 65555, -1);

SYSTEM SYNC REPLICA table_with_version_replicated_2;

DETACH TABLE table_with_version_replicated_1;
DETACH TABLE table_with_version_replicated_2;
ATTACH TABLE table_with_version_replicated_2;
ATTACH TABLE table_with_version_replicated_1;

SELECT * FROM table_with_version_replicated_1 FINAL ORDER BY key;

SYSTEM SYNC REPLICA table_with_version_replicated_2;

SHOW CREATE TABLE table_with_version_replicated_2;

SELECT * FROM table_with_version_replicated_2 FINAL ORDER BY key;

DROP TABLE IF EXISTS table_with_version_replicated_1;
DROP TABLE IF EXISTS table_with_version_replicated_2;
