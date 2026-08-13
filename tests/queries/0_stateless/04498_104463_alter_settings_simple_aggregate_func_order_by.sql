-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104463

DROP TABLE IF EXISTS t_104463;
SET allow_suspicious_primary_key = 1;
CREATE TABLE t_104463 (key Int32, value SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree PRIMARY KEY key ORDER BY (key, value) SETTINGS merge_with_ttl_timeout = 30;
SET allow_suspicious_primary_key = 0;

-- ALTERs that cannot change the sorting key must succeed.
ALTER TABLE t_104463 MODIFY SETTING merge_with_ttl_timeout = 60;
ALTER TABLE t_104463 MODIFY COMMENT 'c';
ALTER TABLE t_104463 MODIFY COLUMN value CODEC(ZSTD);

-- ALTERs that do change the sorting key must still be rejected.
ALTER TABLE t_104463 MODIFY COLUMN key SimpleAggregateFunction(any, Int32); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
ALTER TABLE t_104463 ADD COLUMN value2 SimpleAggregateFunction(sum, UInt64), MODIFY ORDER BY (key, value2); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
DROP TABLE t_104463;

-- StorageReplicatedMergeTree::alter shares the gate (snapshots the key before apply); cover its skip path.
SET allow_suspicious_primary_key = 1;
CREATE TABLE t_104463 (key Int32, value SimpleAggregateFunction(sum, UInt64))
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{database}/t_104463', 'r1') ORDER BY (key, value);
SET allow_suspicious_primary_key = 0;
ALTER TABLE t_104463 MODIFY SETTING merge_with_ttl_timeout = 60;
DROP TABLE t_104463;
