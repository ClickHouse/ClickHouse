-- JOIN runtime-filter pruning combined with the index ranges refiner
-- (use_indexes_refiner_in_read_pools). The refiner is not used when runtime filters drive
-- index pruning (its eager per-part build could snapshot a not-yet-published filter), so
-- the pruning events and the amount of read data must not depend on the setting.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
-- The probe side has 2000 rows, so the randomized threshold must not skip the filter.
SET join_runtime_filter_min_probe_rows = 0;
-- The asserted granule-pruning counts depend on which side builds the runtime filter, so the
-- randomized join-order perturbation must be off (it reproducibly flips the result, see issue).
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_indexes_refiner_in_read_pools = 1;
SET query_plan_join_swap_table = 'false';
-- Left-side join pruning is intentionally disabled under parallel replicas, so pin PR off to
-- exercise the feature (the ParallelReplicas CI job otherwise forces it on).
SET enable_parallel_replicas = 0;
-- The two runs execute the same predicate, so a query condition cache hit in the second run
-- would prune extra granules and break the read_rows parity check.
SET use_query_condition_cache = 0;
-- `PartsSplitter` fault injection is a per-query coin flip: when it fires, the part's ranges are
-- split by primary key into layers read in order, and the mark on the layer boundary is read twice.
-- The two runs would then flip independently and read a different number of rows.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS refiner_fact;
DROP TABLE IF EXISTS refiner_dim;

CREATE TABLE refiner_fact (id UInt64, k UInt64, v UInt64, INDEX idx_k k TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE refiner_dim (k UInt64, tag String) ENGINE = MergeTree ORDER BY k;

INSERT INTO refiner_fact SELECT number, number, number FROM numbers(2000);
INSERT INTO refiner_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

-- The result must not depend on whether the refiner is enabled.
SELECT d.tag, sum(f.v)
FROM refiner_fact AS f
INNER JOIN refiner_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
GROUP BY d.tag
SETTINGS log_comment = '04653_refiner_on';

SELECT d.tag, sum(f.v)
FROM refiner_fact AS f
INNER JOIN refiner_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
GROUP BY d.tag
FORMAT Null
SETTINGS log_comment = '04653_refiner_off', use_indexes_refiner_in_read_pools = 0;

SYSTEM FLUSH LOGS query_log;

-- Runtime-filter pruning must still drop granules with the refiner enabled,
-- and both runs must read the same number of rows.
SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04653_refiner_on'
    AND type = 'QueryFinish';

SELECT
    (SELECT argMax(read_rows, event_time)
     FROM system.query_log
     WHERE current_database = currentDatabase() AND log_comment = '04653_refiner_on' AND type = 'QueryFinish') =
    (SELECT argMax(read_rows, event_time)
     FROM system.query_log
     WHERE current_database = currentDatabase() AND log_comment = '04653_refiner_off' AND type = 'QueryFinish');

DROP TABLE refiner_fact;
DROP TABLE refiner_dim;
