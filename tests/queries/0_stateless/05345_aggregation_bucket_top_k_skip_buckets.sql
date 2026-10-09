-- The final merge of `GROUP BY ... ORDER BY count() DESC LIMIT n` skips the two-level buckets whose groups cannot reach
-- the result: it bounds the counts of a bucket by sums over bins of the key hash. 50 heavy keys with the distinct counts
-- 200 + 4 * k are spread over four blocks, and so over all aggregating streams, next to 100000 keys seen once, so every
-- top below is exact. The last cells print how many buckets each marked query skipped.

SET max_threads = 4;
SET max_block_size = 8192;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_rows_to_group_by = 0;
SET enable_adaptive_aggregator = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 0;
SET enable_packed_string_keys_in_aggregation = 1;
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;

DROP TABLE IF EXISTS t_bucket_top_k_skip_rows;
DROP TABLE IF EXISTS t_bucket_top_k_skip;

CREATE TABLE t_bucket_top_k_skip_rows ENGINE = Memory AS
SELECT k, concat('heavy-', toString(k)) AS s, cityHash64(k, r) % 4 AS block FROM (SELECT number AS k FROM numbers(50)) ARRAY JOIN range(200 + 4 * k) AS r
UNION ALL
SELECT 1000000 + number, concat('tail-', toString(number)), number % 4 FROM numbers(100000);

CREATE TABLE t_bucket_top_k_skip (k UInt64, s String) ENGINE = Memory;
INSERT INTO t_bucket_top_k_skip SELECT k, s FROM t_bucket_top_k_skip_rows WHERE block = 0;
INSERT INTO t_bucket_top_k_skip SELECT k, s FROM t_bucket_top_k_skip_rows WHERE block = 1;
INSERT INTO t_bucket_top_k_skip SELECT k, s FROM t_bucket_top_k_skip_rows WHERE block = 2;
INSERT INTO t_bucket_top_k_skip SELECT k, s FROM t_bucket_top_k_skip_rows WHERE block = 3;
DROP TABLE t_bucket_top_k_skip_rows;

SELECT 'numeric key';
SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS log_comment = 'bucket_top_k_skip 1 numeric key';

SELECT 'String key';
SELECT s, count() AS c FROM t_bucket_top_k_skip GROUP BY s ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'bucket_top_k_skip 2 String key';

SELECT 'two keys';
SELECT k, s, count() AS c FROM t_bucket_top_k_skip GROUP BY k, s ORDER BY c DESC LIMIT 3 SETTINGS log_comment = 'bucket_top_k_skip 3 two keys';

SELECT 'offset';
SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c DESC LIMIT 3 OFFSET 45;

SELECT 'limit above the number of heavy keys';
SELECT sum(c), count() FROM (SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c DESC LIMIT 60);

SELECT 'ascending';
SELECT count() FROM (SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c ASC LIMIT 10) WHERE c = 1
SETTINGS log_comment = 'bucket_top_k_skip 4 ascending';

SELECT 'legacy String table';
SELECT s, count() AS c FROM t_bucket_top_k_skip GROUP BY s ORDER BY c DESC LIMIT 5
SETTINGS enable_packed_string_keys_in_aggregation = 0, log_comment = 'bucket_top_k_skip 5 legacy String table';

SELECT 'adaptive aggregator';
SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0, log_comment = 'bucket_top_k_skip 9 adaptive aggregator';

-- One stream holds three keys only and stays single-level, while the other one becomes two-level.
SELECT 'a single-level stream';
SELECT k, count() AS c FROM
(
    SELECT 2000000 + number AS k FROM numbers(3) ARRAY JOIN range(1000 + number) AS r
    UNION ALL
    SELECT if(number < 10, number, 1000000 + number) AS k FROM numbers(100010) ARRAY JOIN range(if(number < 10, 500 + 10 * number, 1)) AS r
)
GROUP BY k ORDER BY c DESC LIMIT 5
SETTINGS max_block_size = 1000000, group_by_two_level_threshold = 1000, log_comment = 'bucket_top_k_skip 6 a single-level stream';

-- A limit that skips nothing does not stop a smaller limit over the same aggregation from skipping.
SELECT 'a larger limit before';
SET collect_hash_table_stats_during_aggregation = 1;
SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c DESC LIMIT 60 SETTINGS log_comment = 'bucket_top_k_skip 7 a larger limit' FORMAT Null;
SELECT k, count() AS c FROM t_bucket_top_k_skip GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS log_comment = 'bucket_top_k_skip 8 after a larger limit';
SET collect_hash_table_stats_during_aggregation = 0;

-- Every key is seen once, so the first run skips nothing and the next run of the same query does not count the bins.
SELECT 'counting cooldown';
CREATE TABLE t_bucket_top_k_cooldown (k UInt64) ENGINE = Memory;
INSERT INTO t_bucket_top_k_cooldown SELECT 1000000 + number FROM numbers(100000);
SET collect_hash_table_stats_during_aggregation = 1;
SELECT k, count() AS c FROM t_bucket_top_k_cooldown GROUP BY k ORDER BY c DESC LIMIT 10 FORMAT Null;
INSERT INTO t_bucket_top_k_cooldown SELECT number % 20 FROM numbers(4000);
SELECT k, count() AS c FROM t_bucket_top_k_cooldown GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS log_comment = 'bucket_top_k_skip 10 cooldown' FORMAT Null;
SET collect_hash_table_stats_during_aggregation = 0;
SELECT k, count() AS c FROM t_bucket_top_k_cooldown GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS log_comment = 'bucket_top_k_skip 11 cooldown without statistics' FORMAT Null;
DROP TABLE t_bucket_top_k_cooldown;

SYSTEM FLUSH LOGS query_log;
SELECT 'skipped buckets';
SELECT
    replaceOne(log_comment, 'bucket_top_k_skip ', ''),
    if(ProfileEvents['AggregationBucketTopKSkippedBuckets'] > 200, 'most', toString(ProfileEvents['AggregationBucketTopKSkippedBuckets']))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'bucket_top_k_skip %'
ORDER BY event_time_microseconds;

SELECT 'adaptive aggregator engaged';
SELECT ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'bucket_top_k_skip 9 adaptive aggregator';

DROP TABLE t_bucket_top_k_skip;
