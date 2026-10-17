-- A seal-gated read must COMBINE the runtime-filter seal refiner with the index refiner
-- (`use_indexes_refiner_in_read_pools`), not replace it: both prune mark ranges at task-cut
-- time, as a chain. The probe table has the join key `k` in the primary key (pruned by the
-- seal) and a projection index on `region` (pruned by the index refiner; a JOIN disables
-- `use_skip_indexes_on_data_read`, so the projection index is the data-read index source
-- available to a gated read). The combined read must drop more marks than either refiner alone.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_seal_gated_reading = 1;
-- The read-time runtime-filter index analysis is redundant for a gated read; keep it out of
-- the contrast (same pin as in 04653_join_seal_gated_reading).
SET enable_join_runtime_filters_index_analysis = 0;
SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET use_indexes_refiner_in_read_pools = 1;
SET enable_parallel_replicas = 0;
-- Keep the default multi-threaded pool and the chosen join sides stable under CI randomization.
SET max_threads = 4, merge_tree_min_rows_for_concurrent_read = 256, merge_tree_min_read_task_size = 1, optimize_read_in_order = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS probe_05338;
DROP TABLE IF EXISTS build_05338;

CREATE TABLE probe_05338
(
    k UInt64,
    region String,
    PROJECTION region_proj INDEX region TYPE basic
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    auto_statistics_types = '';

-- 'hot' rows are k in [40000, 54000): they overlap the build-side key band [50000, 58000)
-- on [50000, 54000), so each refiner alone keeps extra marks the other one drops.
INSERT INTO probe_05338 SELECT number, if(number >= 40000 AND number < 54000, 'hot', 'common') FROM numbers(100000);

CREATE TABLE build_05338 (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO build_05338 SELECT 50000 + number FROM numbers(8000);

-- The gated pipeline shape is unchanged by the index-refiner features.
SELECT count() > 0 FROM (EXPLAIN PIPELINE
    SELECT count() FROM probe_05338 AS p INNER JOIN build_05338 AS b ON p.k = b.k WHERE p.region = 'hot'
) WHERE explain LIKE '%SealGatedRead%';

-- Both refiners chained: the seal drops the marks outside the build-side key band, the index
-- refiner drops the not-'hot' part of the survivors.
SELECT /* refiners_combined */ count() FROM probe_05338 AS p INNER JOIN build_05338 AS b ON p.k = b.k WHERE p.region = 'hot';

-- Seal refiner only (no projection index filtering).
SELECT /* refiners_seal_only */ count() FROM probe_05338 AS p INNER JOIN build_05338 AS b ON p.k = b.k WHERE p.region = 'hot'
SETTINGS optimize_use_projection_filtering = 0, use_indexes_refiner_in_read_pools = 0;

-- Seal refiner with the projection index applied only at reader level (the default
-- use_indexes_refiner_in_read_pools = 0): the chain gets an accounting-only index refiner
-- so the per-part cached index read results still release as the seal drops marks.
SELECT /* refiners_seal_reader_level */ count() FROM probe_05338 AS p INNER JOIN build_05338 AS b ON p.k = b.k WHERE p.region = 'hot'
SETTINGS use_indexes_refiner_in_read_pools = 0;

-- Index refiner only (no gating).
SELECT /* refiners_index_only */ count() FROM probe_05338 AS p INNER JOIN build_05338 AS b ON p.k = b.k WHERE p.region = 'hot'
SETTINGS enable_join_seal_gated_reading = 0;

SYSTEM FLUSH LOGS query_log;

-- ~782 marks in the probe part; the seal alone drops ~719 (outside k in [50000, 58000)), the
-- index alone drops ~672 (not 'hot'), combined ~750. Thresholds leave slack for cut rounding.
WITH dropped_by AS
(
    SELECT
        extract(query, '/\\* (refiners_[a-z_]+) \\*/') AS marker,
        max(ProfileEvents['ReadPoolRangeRefinerDroppedMarks']) AS dropped,
        max(read_rows) AS rows_read
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND event_date >= yesterday() AND query LIKE '%refiners\_%'
    GROUP BY marker
    HAVING marker != ''
)
SELECT
    maxIf(dropped, marker = 'refiners_seal_only') > 700 AS seal_alone_prunes,
    maxIf(dropped, marker = 'refiners_index_only') > 600 AS index_alone_prunes,
    maxIf(dropped, marker = 'refiners_combined') >= maxIf(dropped, marker = 'refiners_seal_only') + 20 AS index_stage_engages_on_gated_read,
    maxIf(dropped, marker = 'refiners_combined') >= maxIf(dropped, marker = 'refiners_index_only') + 40 AS seal_stage_engages,
    maxIf(rows_read, marker = 'refiners_combined') < maxIf(rows_read, marker = 'refiners_seal_only')
        AND maxIf(rows_read, marker = 'refiners_combined') < maxIf(rows_read, marker = 'refiners_index_only') AS combined_reads_less,
    maxIf(dropped, marker = 'refiners_seal_reader_level') > 700 AS accounting_only_chain_prunes_by_seal
FROM dropped_by;

DROP TABLE probe_05338;
DROP TABLE build_05338;
