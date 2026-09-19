SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_pjc;

CREATE TABLE t_pjc (id UInt64, a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    -- Patches must survive the merges to be applied in Join mode on SELECT.
    apply_patches_on_merge = 0,
    -- The v2 format is applied in MergeOnKey mode, which does not use the join cache.
    patch_parts_version = 'v1',
    -- A granule holds at most this many rows, so the 10000 rows of the shared patch
    -- part that the first merged part reads end inside a granule the second one does
    -- not read, and the other way round.
    index_granularity = 8192,
    -- Merge only the two pairs scheduled below, whatever the parts weigh.
    merge_selector_algorithm = 'Manual';

INSERT INTO t_pjc SELECT number, number, number FROM numbers(5000);
INSERT INTO t_pjc SELECT number + 5000, number, number FROM numbers(5000);
INSERT INTO t_pjc SELECT number + 10000, number, number FROM numbers(5000);
INSERT INTO t_pjc SELECT number + 15000, number, number FROM numbers(5000);

-- The narrow patch covers the first part only, so it is in the patch list of the first
-- merged part and not of the second, and the shared wide patch therefore has a
-- different index in the two lists.
UPDATE t_pjc SET a = a + 1 WHERE id < 5000;
UPDATE t_pjc SET b = b + 1 WHERE 1;

SYSTEM SCHEDULE MERGE t_pjc PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SCHEDULE MERGE t_pjc PARTS 'all_3_3_0', 'all_4_4_0';
SYSTEM SYNC MERGES t_pjc;

SELECT name FROM system.parts
WHERE database = currentDatabase() AND table = 't_pjc' AND active AND NOT startsWith(partition_id, 'patch')
ORDER BY name;

-- Patch parts are read in the order of their partition ids, which embed a hash of the
-- updated columns. The index of the shared patch differs between the two merged parts
-- only while the narrow patch sorts first, so assert that instead of assuming it.
SELECT argMin(rows, partition_id) FROM system.parts
WHERE database = currentDatabase() AND table = 't_pjc' AND active AND startsWith(partition_id, 'patch');

-- The failpoint reverses the patch columns of odd-indexed patches, so the two merged
-- parts read the shared patch part with different column orders.
SYSTEM ENABLE FAILPOINT patch_parts_reverse_column_order;

-- One bucket puts every range of the shared patch part into a single cache entry.
SELECT sum(a), sum(b), count() FROM t_pjc
SETTINGS apply_patch_parts_join_cache_buckets = 1, merge_tree_min_read_task_size = 1;

SYSTEM DISABLE FAILPOINT patch_parts_reverse_column_order;

SELECT sum(a), sum(b), count() FROM t_pjc;

DROP TABLE t_pjc;
