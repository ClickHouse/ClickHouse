-- `HashJoin::publishSharedRuntimeFilters` replaces the runtime filter that `BuildRuntimeFilterStep`
-- registered with one that probes the join's own fixed hash table. The publication can run before the
-- last stream-local filter has registered, so the replacement must not snapshot the index-analysis
-- metadata: it keeps the superseded filter as the metadata source instead, and the late registrations
-- complete the exact key values and the `[min, max]` key range there. Otherwise a multi-stream build
-- would nondeterministically lose granule pruning on the probe side.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_runtime_filter_from_fixed_hash_table = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_before_external_join = 0;
SET use_skip_indexes_on_data_read = 1;
SET max_insert_threads = 1;
-- Read the build side with several streams, so several stream-local filters have to be merged.
SET max_threads = 4;

DROP TABLE IF EXISTS sfht_probe;
DROP TABLE IF EXISTS sfht_build;

CREATE TABLE sfht_probe (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
-- A dense `[0, 99]` key range that `HashJoin` converts to a `FixedHashMap`, spread over four parts so
-- the build side is read by more than one stream.
CREATE TABLE sfht_build (k UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO sfht_probe SELECT number, number FROM numbers(10000);
INSERT INTO sfht_build SELECT number FROM numbers(25);
INSERT INTO sfht_build SELECT number + 25 FROM numbers(25);
INSERT INTO sfht_build SELECT number + 50 FROM numbers(25);
INSERT INTO sfht_build SELECT number + 75 FROM numbers(25);

SELECT 'result';
SELECT count(), sum(p.v) FROM sfht_probe AS p INNER JOIN sfht_build AS b ON p.k = b.k
    SETTINGS log_comment = '05246_shared_fixed_hash_table';

-- The control: the same query with index analysis off must return the same rows and prune nothing.
SELECT count(), sum(p.v) FROM sfht_probe AS p INNER JOIN sfht_build AS b ON p.k = b.k
    SETTINGS enable_join_runtime_filters_index_analysis = 0, log_comment = '05246_index_analysis_off';

SYSTEM FLUSH LOGS query_log;
SELECT 'granules dropped';
SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05246_shared_fixed_hash_table', '05246_index_analysis_off')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE sfht_probe;
DROP TABLE sfht_build;
