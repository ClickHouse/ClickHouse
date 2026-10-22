-- Tags: long, zookeeper, no-replicated-database, no-async-insert
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- Tag no-async-insert: Async insert calculate block_id differently, it takes all inserted data into account

SET insert_keeper_fault_injection_probability=0;
DROP TABLE IF EXISTS partitioned_table SYNC;

CREATE TABLE partitioned_table (
    key UInt64,
    partitioner UInt8,
    value String
)
ENGINE ReplicatedMergeTree('/clickhouse/{database}/01650_drop_part_and_deduplication_partitioned_table', '1')
ORDER BY key
PARTITION BY partitioner;

SYSTEM STOP MERGES partitioned_table;

-- With the default new_unified_hash, the deduplication id is computed over the whole inserted
-- block and stored per partition as <partition>_<hash> under the deduplication_hashes znode. So a
-- retry of the identical insert is fully deduplicated; DROP PART removes that partition's entry,
-- after which the same partition can be inserted again while the others stay deduplicated.
INSERT INTO partitioned_table VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 3, 'C');

SELECT '~~~~source parts~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/deduplication_hashes/' ORDER BY value;

INSERT INTO partitioned_table VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 3, 'C'); -- identical insert, must be fully deduplicated

SELECT '~~~~parts after deduplicated retry~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/deduplication_hashes/' ORDER BY value;

ALTER TABLE partitioned_table DROP PART '3_0_0_0';

SELECT '~~~~parts after drop 3_0_0_0~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/deduplication_hashes/' ORDER BY value;

INSERT INTO partitioned_table VALUES (1, 1, 'A'), (2, 2, 'B'), (3, 3, 'C'); -- partitions 1 and 2 stay deduplicated; partition 3 was dropped, so it is inserted again

SELECT '~~~~parts after retry following drop~~~~~';

SELECT partition_id, name FROM system.parts WHERE table = 'partitioned_table' AND database = currentDatabase() and active ORDER BY name;

SELECT substring(name, 1, 2), value FROM system.zookeeper WHERE path='/clickhouse/' || currentDatabase() || '/01650_drop_part_and_deduplication_partitioned_table/deduplication_hashes/' ORDER BY value;

DROP TABLE IF EXISTS partitioned_table SYNC;
