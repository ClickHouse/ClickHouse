DROP TABLE IF EXISTS versioned_collapsing_table;

CREATE TABLE versioned_collapsing_table(
  d Date,
  key1 UInt64,
  key2 UInt32,
  value String,
  sign Int8,
  version UInt16
)
ENGINE = ReplicatedVersionedCollapsingMergeTree('/clickhouse/versioned_collapsing_table', '1', sign, version)
PARTITION BY d
ORDER BY (key1, key2);

INSERT INTO versioned_collapsing_table VALUES (toDate('2019-10-10'), 1, 1, 'Hello', -1, 1);

SELECT value FROM system.zookeeper WHERE path = '/clickhouse/versioned_collapsing_table' and name = 'metadata';

SELECT COUNT() FROM versioned_collapsing_table;

DETACH TABLE versioned_collapsing_table;
ATTACH TABLE versioned_collapsing_table;

SELECT COUNT() FROM versioned_collapsing_table;

DROP TABLE IF EXISTS versioned_collapsing_table;
