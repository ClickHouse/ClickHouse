DROP POLICY IF EXISTS url_na_log_policy0 ON url_na_log;
DROP TABLE IF EXISTS url_na_log;

CREATE TABLE url_na_log
(
    `SiteId` UInt32,
    `DateVisit` Date
)
ENGINE = MergeTree
PRIMARY KEY SiteId
ORDER BY (SiteId, DateVisit)
SETTINGS index_granularity = 1000, min_bytes_for_wide_part = 0;

CREATE ROW POLICY url_na_log_policy0 ON url_na_log FOR SELECT USING (DateVisit < '2022-08-11') OR (DateVisit > '2022-08-19') TO default;

INSERT INTO url_na_log
SETTINGS max_insert_block_size = 200000
SELECT
    209,
    CAST('2022-08-09', 'Date') + toIntervalDay(intDiv(number, 10000))
FROM numbers(130000)
SETTINGS max_insert_block_size = 200000;

SET max_block_size = 1048576, max_threads = 1, enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3, parallel_replicas_min_number_of_rows_per_replica=10000;

EXPLAIN ESTIMATE
SELECT count()
FROM url_na_log
PREWHERE (DateVisit >= toFixedString('2022-08-10', 10)) AND (DateVisit <= '2022-08-20')
SETTINGS parallel_replicas_local_plan=0;

-- here parallel replicas uses local snapshot as working set
-- so, the estimation can be done
EXPLAIN ESTIMATE
SELECT count()
FROM url_na_log
PREWHERE (DateVisit >= toFixedString('2022-08-10', 10)) AND (DateVisit <= '2022-08-20')
SETTINGS allow_experimental_analyzer=1, parallel_replicas_local_plan=1;

DROP POLICY url_na_log_policy0 ON url_na_log;
DROP TABLE url_na_log;
