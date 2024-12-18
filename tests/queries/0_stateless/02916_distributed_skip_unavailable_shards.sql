-- Tags: shard, no-fasttest

DROP TABLE IF EXISTS table_02916;
DROP TABLE IF EXISTS table_02916_distributed;

CREATE TABLE table_02916
(
    `ID` UInt32,
    `Name` String
)
ENGINE = MergeTree
ORDER BY ID;

INSERT INTO table_02916 VALUES (1234, 'abcd');

CREATE TABLE table_02916_distributed
(
    `ID` UInt32,
    `Name` String
)
ENGINE = Distributed(test_unavailable_shard, currentDatabase(), table_02916, rand())
SETTINGS skip_unavailable_shards = 1;

SET send_logs_level='fatal';
SELECT *, _shard_num FROM table_02916_distributed;

DROP TABLE table_02916_distributed;
DROP TABLE table_02916;
