-- Tags: replica

DROP TABLE IF EXISTS replicated_mutations_empty_partitions SYNC;

CREATE TABLE replicated_mutations_empty_partitions
(
    key UInt64,
    value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/test/'||currentDatabase()||'/01586_replicated_mutations_empty_partitions/{shard}', '{replica}')
ORDER BY key
PARTITION by key;

-- insert_keeper* settings are adjusted since several actual inserts are happening behind one statement due to partitioning i.e. inserts in different partitions
INSERT INTO replicated_mutations_empty_partitions SETTINGS insert_keeper_fault_injection_probability=0 SELECT number, toString(number) FROM numbers(10);

SELECT count(distinct value) FROM replicated_mutations_empty_partitions;

SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/test/'||currentDatabase()||'/01586_replicated_mutations_empty_partitions/'||getMacro('shard')||'/block_numbers';

ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '3';
ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '4';
ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '5';
ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '9';

-- still ten records
SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/test/'||currentDatabase()||'/01586_replicated_mutations_empty_partitions/'||getMacro('shard')||'/block_numbers';

ALTER TABLE replicated_mutations_empty_partitions MODIFY COLUMN value UInt64 SETTINGS replication_alter_partitions_sync=2;

SELECT sum(value) FROM replicated_mutations_empty_partitions;

SHOW CREATE TABLE replicated_mutations_empty_partitions;

DROP TABLE IF EXISTS replicated_mutations_empty_partitions SYNC;
