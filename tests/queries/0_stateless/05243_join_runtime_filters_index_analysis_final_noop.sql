-- The second-pass granule pruning of `enable_join_runtime_filters_index_analysis` is not implemented
-- for a left side read with `FINAL`: `initializePipeline` skips it for `FINAL` reads, and
-- `optimizeLazyFinal` rebuilds such a read without the descriptors. This test pins that documented
-- no-op: the `FINAL` query returns the correct deduplicated result, and no granules are considered
-- for the runtime filter pruning, while the same query without `FINAL` does prune.

DROP TABLE IF EXISTS rf_final_fact;
DROP TABLE IF EXISTS rf_final_dim;

CREATE TABLE rf_final_fact (id UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE rf_final_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES rf_final_fact;
INSERT INTO rf_final_fact SELECT number, number FROM numbers(2000);
-- A second version of every hot row, so that `FINAL` has something to deduplicate.
INSERT INTO rf_final_fact SELECT number, number * 10 FROM numbers(64);
INSERT INTO rf_final_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET join_runtime_filter_min_probe_rows = 0;
-- Which side builds the runtime filter decides whether the left side can be pruned at all, so the
-- randomized join-order perturbation has to be off.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 0;

-- Control: without `FINAL` the local read prunes (both versions of the hot rows are joined).
SELECT 'no_final', count(), sum(f.v)
FROM rf_final_fact AS f INNER JOIN rf_final_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05243_no_final';

-- `FINAL`: only the latest version of every hot row, and no runtime filter granule pruning.
SELECT 'final', count(), sum(f.v)
FROM rf_final_fact AS f FINAL INNER JOIN rf_final_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05243_final';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0 AS granules_considered,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0 AS granules_dropped
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05243_no_final', '05243_final')
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY log_comment;

DROP TABLE rf_final_fact;
DROP TABLE rf_final_dim;
