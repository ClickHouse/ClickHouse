-- Tags: long, no-shared-merge-tree

DROP TABLE IF EXISTS test_01640;
DROP TABLE IF EXISTS restore_01640;

CREATE TABLE test_01640(i Int64, d Date, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{shard}/tables/test_01640','{replica}')
PARTITION BY toYYYYMM(d) ORDER BY i;

insert into test_01640 values (1, '2021-01-01','some');

CREATE TABLE restore_01640(i Int64, d Date, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/{shard}/tables/restore_01640','{replica}')
PARTITION BY toYYYYMM(d) ORDER BY i;

ALTER TABLE restore_01640 FETCH PARTITION tuple(toYYYYMM(toDate('2021-01-01')))
  FROM '/clickhouse/{database}/{shard}/tables/test_01640' SETTINGS insert_keeper_fault_injection_probability=0;

SELECT partition_id
FROM system.detached_parts
WHERE (table = 'restore_01640') AND (database = currentDatabase());

ALTER TABLE restore_01640 ATTACH PARTITION tuple(toYYYYMM(toDate('2021-01-01'))) SETTINGS insert_keeper_fault_injection_probability=0;;

SELECT partition_id
FROM system.detached_parts
WHERE (table = 'restore_01640') AND (database = currentDatabase());

SELECT _part, * FROM restore_01640;

DROP TABLE test_01640;
DROP TABLE restore_01640;
