-- Tags: long, zookeeper, no-replicated-database
-- Tag no-replicated-database: Fails due to additional replicas or shards

DROP TABLE IF EXISTS partitioned_table;

CREATE TABLE partitioned_table (
    key UInt64,
    partitioner UInt8,
    value String
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/01650_drop_part_and_deduplication_partitioned_table', '1')
ORDER BY key
PARTITION BY partitioner;

SYSTEM STOP MERGES partitioned_table;

INSERT INTO partitioned_table VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 3, 'C');
INSERT INTO partitioned_table VALUES (11, 1, 'AA'), (22, 2, 'BB'), (33, 3, 'CC');

SELECT '~~~~source parts~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/blocks/' ORDER BY value;

INSERT INTO partitioned_table VALUES (33, 3, 'CC'); -- must be deduplicated

SELECT '~~~~parts after deduplication~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/blocks/' ORDER BY value;

ALTER TABLE partitioned_table DROP PART '3_1_1_0';

SELECT '~~~~parts after drop 3_1_1_0~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/blocks/' ORDER BY value;

INSERT INTO partitioned_table VALUES (33, 3, 'CC'); -- mustn't be deduplicated

SELECT '~~~~parts after new part without deduplication~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/blocks/' ORDER BY value;

DROP TABLE IF EXISTS partitioned_table;
