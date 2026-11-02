-- Tags: no-random-merge-tree-settings

-- A compact part keeps no per-column sizes, so the input-bytes estimate cannot read them off the
-- part and has to derive the compression ratio. Deriving it by compressing a sample with the
-- built-in default codec is wrong whenever the data is stored with another one: for `ZSTD` the
-- estimate came out ~1.5x too large, which inflates `effective_max_reading_threads` and flips the
-- decision to enable parallel replicas.

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=4, max_block_size=8192;

SET enable_analyzer=1;

DROP TABLE IF EXISTS t_default_codec;
DROP TABLE IF EXISTS t_column_codec;

-- Statistics are disabled so that reading them is not accounted in `ReadCompressedBytes`.
-- The codec is set two different ways, because the two are resolved differently: as the part's
-- default codec, and as a codec of an individual column overriding it.
CREATE TABLE t_default_codec(a UInt64, b UInt64) ENGINE=MergeTree ORDER BY a
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18, default_compression_codec='ZSTD(1)';

CREATE TABLE t_column_codec(a UInt64 CODEC(ZSTD(1)), b UInt64 CODEC(ZSTD(1))) ENGINE=MergeTree ORDER BY a
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18;

INSERT INTO t_default_codec SELECT number, number % 1000 FROM numbers(1e6);
INSERT INTO t_column_codec SELECT number, number % 1000 FROM numbers(1e6);

SELECT a, b FROM t_default_codec FORMAT Null SETTINGS log_comment='query_default_codec';
SELECT a, b FROM t_column_codec FORMAT Null SETTINGS log_comment='query_column_codec';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Both parts are stored with `ZSTD`, so the estimate has to be close to what was really read.
-- Estimating with the default codec instead lands around 1.5x, well outside this bound.
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
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 1.2;

DROP TABLE t_default_codec;
DROP TABLE t_column_codec;
