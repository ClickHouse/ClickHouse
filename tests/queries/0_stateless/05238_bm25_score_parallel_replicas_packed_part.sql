-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test configures parallel replicas explicitly.

-- With parallel replicas and a single thread, one reader handles several tasks of a part and gets
-- new mark ranges between them. The doc-lengths stream of `_bm25_score` must extend its readable
-- range too: in a packed part it was cut at the first task's last mark (`CANNOT_READ_ALL_DATA`).

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_mark_segment_size = 128;
SET max_threads = 1;

DROP TABLE IF EXISTS tab_bm25_pr_packed;

CREATE TABLE tab_bm25_pr_packed
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
PARTITION BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = '1Gi', add_minmax_index_for_numeric_columns = 0, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_pr_packed SELECT 1, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(100000);
INSERT INTO tab_bm25_pr_packed SELECT 2, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(100000);
INSERT INTO tab_bm25_pr_packed SELECT 3, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(100000);

SELECT round(_bm25_score, 2) AS score, count() FROM tab_bm25_pr_packed WHERE hasToken(str, 'error') GROUP BY score ORDER BY score;

DROP TABLE tab_bm25_pr_packed;
