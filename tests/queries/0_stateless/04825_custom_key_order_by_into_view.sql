-- Regression test: `pushOrderByIntoView` must not push `ORDER BY`/`LIMIT` into a view when the
-- parallel-replicas custom-key filter applies on this replica. The filter is appended as a planner
-- `where_filter` above the view subquery, so pushing the `LIMIT` into the view would truncate to
-- the globally top rows before the filter discards the rows outside this replica's share of the
-- key space: the inner query returns only `k = 0`, the custom-key filter for the second replica
-- (`k >= 500`) drops it, and the query returns no row even though `500` is the correct top row of
-- that replica's range. This models the replica-side planning directly, like
-- `04620_parallel_replicas_custom_key_trivial_limit`.

DROP TABLE IF EXISTS t_04825;
DROP VIEW IF EXISTS v_04825;

CREATE TABLE t_04825 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_04825 SELECT number FROM numbers(1000);
CREATE VIEW v_04825 AS SELECT k FROM t_04825;

-- The plan must keep a single `Sorting` step: the pushdown would add a second one inside the view
-- subquery. (For a plain view over a table exposing the key column the values happen to stay
-- correct even with the pushdown, because the custom-key filter is also applied to the storage
-- read inside the view, below the pushed `ORDER BY`/`LIMIT`; the pushdown must not rely on that.)
SELECT if(countIf(explain LIKE '%Sorting%') = 1, 'not pushed', 'pushed')
FROM (EXPLAIN SELECT k FROM v_04825 ORDER BY k ASC LIMIT 1 SETTINGS
    max_threads = 1, max_block_size = 65536,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 1,
    enable_analyzer = 1);

-- Control: without the custom-key filter the pushdown still fires (a second `Sorting` step inside
-- the view subquery), so the check above stays meaningful.
SELECT if(countIf(explain LIKE '%Sorting%') = 2, 'pushed', 'not pushed')
FROM (EXPLAIN SELECT k FROM v_04825 ORDER BY k ASC LIMIT 1 SETTINGS max_threads = 1, enable_analyzer = 1);

-- The second replica owns the upper half of the key range: its top row is `500`, which is not the
-- globally top row, so a `LIMIT` pushed into the view would starve it.
SELECT k FROM v_04825 ORDER BY k ASC LIMIT 1 SETTINGS
    max_threads = 1, max_block_size = 65536,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 1,
    enable_analyzer = 1;

-- The first replica owns the lower half: its top row coincides with the global one.
SELECT k FROM v_04825 ORDER BY k ASC LIMIT 1 SETTINGS
    max_threads = 1, max_block_size = 65536,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 0,
    enable_analyzer = 1;

DROP VIEW v_04825;
DROP TABLE t_04825;
