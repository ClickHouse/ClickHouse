SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree=1;
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET parallel_replicas_mark_segment_size = 128;
SET parallel_replicas_min_number_of_rows_per_replica = 1000;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 8
)
ENGINE = MergeTree ORDER BY id PARTITION BY id;

INSERT INTO tab SELECT 1, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(0, 100000);
INSERT INTO tab SELECT 2, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(100000, 100000);
INSERT INTO tab SELECT 3, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(200000, 100000);

SELECT count(), sum(id) FROM tab WHERE hasAnyTokens(str, ['34567', '134567', '234567']);
SELECT count(), sum(id) FROM tab WHERE str LIKE '% 34567 %';

SYSTEM FLUSH LOGS query_log;

SELECT
    sum(ProfileEvents['ParallelReplicasUsedCount']) > 0,
    sum(ProfileEvents['TextIndexUsedEmbeddedPostings']) > 0
FROM system.query_log
WHERE (current_database = currentDatabase() OR position(query, currentDatabase()) > 0) AND query LIKE '%SELECT%tab%hasAnyTokens%' AND type = 'QueryFinish';

DROP TABLE tab;
