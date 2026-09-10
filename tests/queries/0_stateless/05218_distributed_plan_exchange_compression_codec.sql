-- Tags: no-fasttest, no-old-analyzer
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.
-- no-old-analyzer: distributed planning requires the analyzer.

-- The exchange between the tasks of a distributed plan compresses its packets with the codec of
-- `network_compression_method`. The setting reaches the sending tasks on the workers, not only the
-- initiator. The check reads the bytes the serializers of the tasks produce: with `NONE` the packets
-- are bigger than with `LZ4` or `ZSTD`.

DROP TABLE IF EXISTS t_exchange_codec;
CREATE TABLE t_exchange_codec (k String, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_exchange_codec SELECT concat('key ', toString(number % 1000), ' with some repeated words'), number FROM numbers(100000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2;
SET log_processors_profiles = 1;

SELECT count() FROM (SELECT k, count() FROM t_exchange_codec GROUP BY k)
  SETTINGS network_compression_method = 'NONE', log_comment = '05218_codec_none';
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec GROUP BY k)
  SETTINGS network_compression_method = 'LZ4', log_comment = '05218_codec_lz4';
SELECT count() FROM (SELECT k, count() FROM t_exchange_codec GROUP BY k)
  SETTINGS network_compression_method = 'ZSTD', network_zstd_compression_level = 1, log_comment = '05218_codec_zstd';

-- The log queries below are not the subject of the test; they run without the distributed plan.
SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- The scan of the processor log starts at the first of the three queries: in a busy test run the log
-- holds millions of rows per minute, and a wider scan would hit the read limit of the test profile.
WITH roots AS (
    SELECT query_id, log_comment, query_start_time FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment IN ('05218_codec_none', '05218_codec_lz4', '05218_codec_zstd') AND type = 'QueryFinish'),
packet_bytes AS (
    SELECT roots.log_comment AS run, sum(output_bytes) AS bytes
    FROM system.processors_profile_log AS profiles
    INNER JOIN roots ON profiles.initial_query_id = roots.query_id
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
      AND event_time >= (SELECT min(query_start_time) FROM roots)
      AND name = 'StreamingExchangeSerializingTransform'
    GROUP BY run)
SELECT 'uncompressed packets are bigger than LZ4 and ZSTD:',
    (SELECT bytes FROM packet_bytes WHERE run = '05218_codec_none') > (SELECT bytes FROM packet_bytes WHERE run = '05218_codec_lz4'),
    (SELECT bytes FROM packet_bytes WHERE run = '05218_codec_none') > (SELECT bytes FROM packet_bytes WHERE run = '05218_codec_zstd');

DROP TABLE t_exchange_codec;
