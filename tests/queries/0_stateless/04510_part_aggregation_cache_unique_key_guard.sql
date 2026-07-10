-- Tags: no-parallel, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- no-parallel: asserts the global `system.part_aggregation_cache` count and issues `SYSTEM DROP PART AGGREGATION CACHE`.
-- The remaining tags are the standard `UNIQUE KEY` restrictions (unsupported on Ordinary/Replicated/SharedMergeTree/object storage).

-- Fail-closed guard: the experimental per-part aggregation cache must not run on `UNIQUE KEY` tables.
-- Their per-part delete bitmap is versioned by `snapshot_csn` and can change without renaming the part
-- (a later insert of an existing key marks the old row deleted in an existing part), while the cache key
-- is only `{query_hash, table_id, part_name}`. Reusing a state cached before the bitmap changed would
-- return rows that are deleted in the newer snapshot, so `optimizeUsePartAggregationCache` skips the
-- optimization entirely for these tables. This test checks that a `GROUP BY` over a `UNIQUE KEY` table
-- is answered correctly and populates nothing in the cache (the guard fires); without the guard the two
-- parts below would be cached and `system.part_aggregation_cache` would be non-empty.

SET allow_experimental_analyzer = 0, allow_experimental_unique_key = 1, allow_experimental_part_aggregation_cache = 1,
    optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0,
    max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0,
    async_insert = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_pac_unique_key;
CREATE TABLE t_pac_unique_key (k UInt32, id UInt32, v UInt64) ENGINE = MergeTree ORDER BY k UNIQUE KEY (id);
INSERT INTO t_pac_unique_key VALUES (1, 100, 10), (2, 200, 20);
INSERT INTO t_pac_unique_key VALUES (1, 300, 40), (2, 400, 50);

-- The `GROUP BY` must be correct, and the guard must keep the cache empty for the `UNIQUE KEY` table.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT k, sum(v) FROM t_pac_unique_key GROUP BY k ORDER BY k;
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_pac_unique_key;
SYSTEM DROP PART AGGREGATION CACHE;
