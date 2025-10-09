SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET allow_prefetched_read_pool_for_remote_filesystem = 1;
SET remote_filesystem_read_prefetch = 1;
SET remote_filesystem_read_method = 'threadpool';

DROP TABLE IF EXISTS t_text_index_prefetch;

CREATE TABLE t_text_index_prefetch
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 8
)
ENGINE = MergeTree ORDER BY id PARTITION BY id
SETTINGS storage_policy = 's3_cache';

INSERT INTO t_text_index_prefetch SELECT 1, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(0, 100000);
INSERT INTO t_text_index_prefetch SELECT 2, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(100000, 100000);
INSERT INTO t_text_index_prefetch SELECT 3, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(200000, 100000);

SELECT count(), sum(id) FROM t_text_index_prefetch WHERE hasAnyTokens(str, ['34567', '134567', '234567']);

SYSTEM FLUSH LOGS;

SELECT
    ProfileEvents['RemoteFSPrefetchedReads'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%SELECT count(), sum(id) FROM t_text_index_prefetch%' AND type = 'QueryFinish';

DROP TABLE IF EXISTS t_text_index_prefetch;
