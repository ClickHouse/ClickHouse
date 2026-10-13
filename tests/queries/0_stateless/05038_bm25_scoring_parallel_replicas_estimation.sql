-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test configures parallel replicas explicitly and toggles
--   `parallel_replicas_min_number_of_rows_per_replica` per query, so the test runner must
--   not wrap the test in its own parallel-replicas mode.

-- With parallel replicas, a positive `parallel_replicas_min_number_of_rows_per_replica` makes the
-- initiator run a "row count estimation" index analysis in the planner, before the query plan
-- optimization that rewrites `bm25()` runs, so the text index granules of the estimation pass are
-- analyzed before the scoring predicates are known and are then reused by the data read. The scores
-- must not depend on it: the reader decodes the term frequencies of the scoring tokens itself.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_mark_segment_size = 128;

DROP TABLE IF EXISTS tab_bm25_pr;

CREATE TABLE tab_bm25_pr
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
PARTITION BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0, allow_experimental_text_index_scoring = 1;

-- Three identical parts (one per partition). 10% of the rows contain the token twice, 10% once and
-- the rest not at all: the two distinct term frequencies produce two distinct scores, so scoring
-- from a granule with dropped term frequencies would collapse the two groups into one.
INSERT INTO tab_bm25_pr SELECT 1, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(100000);
INSERT INTO tab_bm25_pr SELECT 2, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(100000);
INSERT INTO tab_bm25_pr SELECT 3, concat(toString(number), multiIf(number % 10 = 0, ' error error', number % 10 = 5, ' error', ' noise')) FROM numbers(100000);

-- No estimation pass: the reference behavior.
SELECT round(bm25(), 2) AS score, count() FROM tab_bm25_pr WHERE hasToken(str, 'error') GROUP BY score ORDER BY score
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0;

-- The estimation pass runs and concludes that one replica is enough: the query falls back to a
-- regular read that reuses the estimation's analysis result, including the text index granules it
-- deserialized. The scores must not change.
SELECT round(bm25(), 2) AS score, count() FROM tab_bm25_pr WHERE hasToken(str, 'error') GROUP BY score ORDER BY score
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1000000000;

-- The estimation pass runs and parallel replicas engage.
SELECT round(bm25(), 2) AS score, count() FROM tab_bm25_pr WHERE hasToken(str, 'error') GROUP BY score ORDER BY score
SETTINGS parallel_replicas_min_number_of_rows_per_replica = 1;

DROP TABLE tab_bm25_pr;
