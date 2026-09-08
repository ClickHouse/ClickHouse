-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test configures parallel replicas explicitly, so the test runner must
--   not wrap the test in its own parallel-replicas mode.

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
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
    INDEX idx_str(str) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_scoring = 1;

-- 10% of the rows contain the token twice, 10% once and the rest not at all: two distinct
-- term frequencies produce two distinct scores.
INSERT INTO tab_bm25_auto_pr SELECT number, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(1000);

-- The automatic-parallel-replicas heuristic builds an alternative plan without index analysis; it
-- must not reject the query at planning time. The plan substitution is skipped for queries reading
-- the score column, so the executed (local) plan still fills it.
SELECT count(), uniqExact(round(_bm25_score, 4)) FROM tab_bm25_auto_pr WHERE hasToken(str, 'error');

DROP TABLE tab_bm25_auto_pr;
