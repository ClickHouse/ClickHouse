-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/75604
-- StorageMerge wrapping a Distributed table + another table crashed with
-- "Logical error: Queries with distributed_group_by_no_merge=1 should be processed to Complete stage"
-- when distributed_group_by_no_merge=1 was set, because StorageMerge caps
-- the processing stage to WithMergeableState for multi-table scenarios,
-- and StorageDistributed asserted to_stage == Complete.

DROP TABLE IF EXISTS local_data;
DROP TABLE IF EXISTS dist_table;
DROP TABLE IF EXISTS memory_table;

CREATE TABLE local_data (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO local_data VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TABLE dist_table (id UInt64, value String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_data');
CREATE TABLE memory_table (id UInt64, value String) ENGINE = Memory;
INSERT INTO memory_table VALUES (10, 'x'), (20, 'y');

-- This was crashing the server (LOGICAL_ERROR)
SELECT count() FROM merge(currentDatabase(), '^(dist_table|memory_table)$')
    SETTINGS distributed_group_by_no_merge = 1;

-- Verify correct results with different query types
SELECT sum(id) FROM merge(currentDatabase(), '^(dist_table|memory_table)$')
    SETTINGS distributed_group_by_no_merge = 1;

SELECT id FROM merge(currentDatabase(), '^(dist_table|memory_table)$')
    ORDER BY id
    SETTINGS distributed_group_by_no_merge = 1;

-- Also test with distributed_group_by_no_merge = 2
SELECT count() FROM merge(currentDatabase(), '^(dist_table|memory_table)$')
    SETTINGS distributed_group_by_no_merge = 2;

DROP TABLE local_data;
DROP TABLE dist_table;
DROP TABLE memory_table;
