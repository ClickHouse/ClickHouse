-- `enable_join_runtime_filters_index_analysis` attaches the descriptors for the second-pass granule
-- pruning to the left side's `ReadFromMergeTree` before projections are considered. When the normal
-- projection optimization then replaces that read step with a read from a projection, the descriptors
-- have to be carried over, otherwise a JOIN whose left side is served from a projection silently loses
-- the pruning even though the read stays local.
--
-- The carried-over descriptors are re-checked against the projection's own primary key: a projection
-- sorted by the join key prunes, a projection sorted by an unrelated column does not, and both return
-- the same rows as the read from the base table.

DROP TABLE IF EXISTS rf_proj_fact SYNC;
DROP TABLE IF EXISTS rf_proj_dim SYNC;

-- The base table is sorted by a hash, so a static range on `id` or `v` reads nearly all of its marks,
-- and the matching projection is picked because it reads fewer.
CREATE TABLE rf_proj_fact
(
    k UInt64,
    id UInt64,
    v UInt64,
    PROJECTION p_by_id (SELECT k, id, v ORDER BY id),
    PROJECTION p_by_v (SELECT k, id, v ORDER BY v)
)
ENGINE = MergeTree ORDER BY (k, id) SETTINGS index_granularity = 16;
CREATE TABLE rf_proj_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
INSERT INTO rf_proj_fact SELECT cityHash64(number), number, 1999 - number FROM numbers(2000);
INSERT INTO rf_proj_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
-- Which side builds the runtime filter decides whether the left side can be pruned at all, so the
-- randomized join-order perturbation has to be off.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 0;

-- The static range keeps every hot row, so all three queries return the same rows.
SELECT 'base_table', count(), sum(f.v)
FROM rf_proj_fact AS f INNER JOIN rf_proj_dim AS d ON f.id = d.id
WHERE d.tag = 'hot' AND f.id < 1000
SETTINGS log_comment = '05223_base_table', optimize_use_projections = 0;

-- The projection is sorted by the join key: the carried-over descriptor prunes its granules.
SELECT 'projection_by_join_key', count(), sum(f.v)
FROM rf_proj_fact AS f INNER JOIN rf_proj_dim AS d ON f.id = d.id
WHERE d.tag = 'hot' AND f.id < 1000
SETTINGS log_comment = '05223_projection_by_join_key';

-- The projection is sorted by an unrelated column: the carried-over descriptor cannot prune there
-- and is dropped, the read stays correct.
SELECT 'projection_by_other_column', count(), sum(f.v)
FROM rf_proj_fact AS f INNER JOIN rf_proj_dim AS d ON f.id = d.id
WHERE d.tag = 'hot' AND f.v >= 1000
SETTINGS log_comment = '05223_projection_by_other_column';

SYSTEM FLUSH LOGS query_log;

-- The projection reads really are projection reads.
SELECT
    log_comment,
    arrayMap(p -> splitByChar('.', p)[-1], arraySort(projections)) AS used_projections
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05223_base_table', '05223_projection_by_join_key', '05223_projection_by_other_column')
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY log_comment
SETTINGS enable_join_runtime_filters_index_analysis = 0;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0 AS granules_considered,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0 AS granules_dropped
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05223_base_table', '05223_projection_by_join_key', '05223_projection_by_other_column')
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY log_comment
SETTINGS enable_join_runtime_filters_index_analysis = 0;

DROP TABLE rf_proj_fact SYNC;
DROP TABLE rf_proj_dim SYNC;
