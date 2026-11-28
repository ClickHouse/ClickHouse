-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test configures parallel replicas explicitly, so the test runner must
--   not wrap the test in its own parallel-replicas mode.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 2;

DROP TABLE IF EXISTS tab_bm25_auto_pr;

CREATE TABLE tab_bm25_auto_pr
(
    id UInt32,
    str String,
    INDEX idx_str(str) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

-- 10% of the rows contain the token twice, 10% once and the rest not at all: two distinct
-- term frequencies produce two distinct scores.
INSERT INTO tab_bm25_auto_pr SELECT number, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(1000);

-- The automatic-parallel-replicas heuristic builds an alternative plan without index analysis; it
-- must not reject the query at planning time. The optimization is skipped for a query computing
-- `bm25()`, so the executed (local) plan still computes the score.
SELECT count(), uniqExact(round(bm25(), 4)) FROM tab_bm25_auto_pr WHERE hasToken(str, 'error')
SETTINGS log_comment = 'query_bm25';

-- A query the optimization supports, so that the check below does not pass trivially when the optimization
-- does not run at all.
SELECT count(), sum(id) FROM tab_bm25_auto_pr WHERE id > 5
SETTINGS log_comment = 'query_supported';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- `automatic_parallel_replicas_mode = 2` only collects the dataflow statistics of the queries the optimization
-- supports, so a skipped query leaves both counters at zero.
SELECT
    log_comment,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0,
    ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 15 MINUTE
    AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('query_bm25', 'query_supported')
ORDER BY log_comment;

DROP TABLE tab_bm25_auto_pr;
