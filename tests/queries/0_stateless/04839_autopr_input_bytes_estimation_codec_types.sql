-- Tags: no-random-merge-tree-settings

-- A compact part keeps no per-column sizes, so the input-bytes estimate compresses a sample of every
-- column with the codec that column is stored with. That codec is written per substream, so it must not
-- be resolved against the whole column type: `T64` on a `Tuple` or `GCD` on a `Nullable` is rejected by
-- the codec factory, and the exception used to escape into the read pipeline and fail the query.

SET use_uncompressed_cache=0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=4, max_block_size=8192;

SET enable_analyzer=1;

DROP TABLE IF EXISTS t_codec_tuple;
DROP TABLE IF EXISTS t_codec_nullable;
DROP TABLE IF EXISTS t_codec_array;
DROP TABLE IF EXISTS t_codec_plain;

CREATE TABLE t_codec_tuple(a UInt64, t Tuple(UInt32, Array(UInt64)) CODEC(T64, LZ4)) ENGINE=MergeTree ORDER BY a
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18;

CREATE TABLE t_codec_nullable(a UInt64, n Nullable(Int64) CODEC(GCD, LZ4)) ENGINE=MergeTree ORDER BY a
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18;

CREATE TABLE t_codec_array(a UInt64, arr Array(UInt32) CODEC(Delta, ZSTD(1))) ENGINE=MergeTree ORDER BY a
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18;

CREATE TABLE t_codec_plain(a UInt64, plain Int64 CODEC(T64, ZSTD(1))) ENGINE=MergeTree ORDER BY a
SETTINGS auto_statistics_types='', index_granularity=8192, min_bytes_for_wide_part=1e18;

INSERT INTO t_codec_tuple SELECT number, (number % 1000, [number, number + 1]) FROM numbers(1e5);
INSERT INTO t_codec_nullable SELECT number, number % 100 FROM numbers(1e5);
INSERT INTO t_codec_array SELECT number, [number % 7, number % 11] FROM numbers(1e5);
INSERT INTO t_codec_plain SELECT number, number FROM numbers(1e5);

SELECT a, t FROM t_codec_tuple FORMAT Null;
SELECT a, n FROM t_codec_nullable FORMAT Null;
SELECT a, arr FROM t_codec_array FORMAT Null;
SELECT a, plain FROM t_codec_plain FORMAT Null;

SELECT 'ok';

DROP TABLE t_codec_tuple;
DROP TABLE t_codec_nullable;
DROP TABLE t_codec_array;
DROP TABLE t_codec_plain;
