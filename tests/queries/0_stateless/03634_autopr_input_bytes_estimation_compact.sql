-- Tags: no-random-merge-tree-settings

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

set max_threads=4, max_block_size=8192;

-- For runs with the old analyzer
SET enable_analyzer=1;

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

DROP TABLE IF EXISTS t;

-- Statistics are disabled to avoid accounting for them in `ReadCompressedBytes`
CREATE TABLE t(a UInt64, s String, d Date) ENGINE=MergeTree PARTITION BY toYYYYMM(d) ORDER BY a SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part = 1e18;

INSERT INTO t SELECT number, toString(number), today() - INTERVAL (number % 30) DAY FROM numbers(1e6);

SELECT a FROM t FORMAT Null SETTINGS log_comment='query_1';

SELECT s FROM t FORMAT Null SETTINGS log_comment='query_2';

SELECT s, d FROM t FORMAT Null SETTINGS log_comment='query_3';

SELECT a, s, d FROM t FORMAT Null SETTINGS log_comment='query_4';

SELECT COUNT(*) FROM t WHERE a % 10 = 0 FORMAT Null SETTINGS log_comment='query_5';

SELECT COUNT(*) FROM t WHERE a > 10101 FORMAT Null SETTINGS log_comment='query_6', allow_experimental_projection_optimization=0, optimize_use_implicit_projections=0;

SELECT COUNT(*) FROM t WHERE a <= 10101 FORMAT Null SETTINGS log_comment='query_7', allow_experimental_projection_optimization=0, optimize_use_implicit_projections=0;

SELECT a, s FROM t WHERE s LIKE '1%' ORDER BY a DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_8';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Just checking that the estimation is not too far off
SELECT format('{} {} {}', log_comment, compressed_bytes, statistics_input_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes'] statistics_input_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 2;

DROP TABLE t;
