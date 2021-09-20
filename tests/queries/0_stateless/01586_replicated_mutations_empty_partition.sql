DROP TABLE IF EXISTS replicated_mutations_empty_partitions;

CREATE TABLE replicated_mutations_empty_partitions
(
    key UInt64,
    value String
)
ENGINE = ReplicatedMergeTree('/clickhouse/test/01586_replicated_mutations_empty_partitions', '1')
ORDER BY key
PARTITION by key;

INSERT INTO replicated_mutations_empty_partitions SELECT number, toString(number) FROM numbers(10);

SELECT count(distinct value) FROM replicated_mutations_empty_partitions;

SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/test/01586_replicated_mutations_empty_partitions/block_numbers';

ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '3';
ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '4';
ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '5';
ALTER TABLE replicated_mutations_empty_partitions DROP PARTITION '9';

-- still ten records
SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/test/01586_replicated_mutations_empty_partitions/block_numbers';

ALTER TABLE replicated_mutations_empty_partitions MODIFY COLUMN value UInt64 SETTINGS replication_alter_partitions_sync=2;

SELECT sum(value) FROM replicated_mutations_empty_partitions;

SHOW CREATE TABLE replicated_mutations_empty_partitions;

DROP TABLE IF EXISTS replicated_mutations_empty_partitions;
