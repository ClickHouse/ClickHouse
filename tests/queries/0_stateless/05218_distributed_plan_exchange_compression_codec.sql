-- Tags: no-fasttest
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.

-- The exchange between the tasks of a distributed plan compresses its packets with the codec of
-- `network_compression_method`. The setting reaches the sending tasks on the workers, not only the
-- initiator. A serializer on every stream ahead of the sinks makes the packets, also on the one
-- stream left after the merge of a sorted gather. The checks compare the bytes the serializers
-- produce: with `NONE` the packets are bigger than with `LZ4` or `ZSTD`.

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

-- The window runs on the shuffle buckets with several threads and sends its sorted result to the
-- initiator through a sorted gather: the sending task merges its streams and serializes after the merge.
SET max_threads = 4;
SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t_exchange_codec
  SETTINGS network_compression_method = 'NONE', log_comment = '05218_codec_sink_none' FORMAT Null;
SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t_exchange_codec
  SETTINGS network_compression_method = 'LZ4', log_comment = '05218_codec_sink_lz4' FORMAT Null;
-- The same query at two `ZSTD` levels: the level reaches the sending tasks too.
SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t_exchange_codec
  SETTINGS network_compression_method = 'ZSTD', network_zstd_compression_level = 1, log_comment = '05218_codec_sink_zstd1' FORMAT Null;
SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t_exchange_codec
  SETTINGS network_compression_method = 'ZSTD', network_zstd_compression_level = 19, log_comment = '05218_codec_sink_zstd19' FORMAT Null;

-- The log queries below are not the subject of the test; they run without the distributed plan.
SET make_distributed_plan = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- The scan of the processor log starts at the first of the queries above: in a busy test run the log
-- holds millions of rows per minute, and a wider scan would hit the read limit of the test profile.
CREATE VIEW v_exchange_codec AS
SELECT roots.log_comment AS run, profiles.query_id AS task,
    countIf(name = 'StreamingExchangeSerializingTransform') AS serializers,
    countIf(name LIKE 'StreamingExchangeSink%') AS sinks,
    sumIf(output_bytes, name = 'StreamingExchangeSerializingTransform') AS serializer_bytes
FROM system.processors_profile_log AS profiles
INNER JOIN (
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment IN ('05218_codec_none', '05218_codec_lz4', '05218_codec_zstd', '05218_codec_sink_none', '05218_codec_sink_lz4', '05218_codec_sink_zstd1', '05218_codec_sink_zstd19')
      AND type = 'QueryFinish') AS roots ON profiles.initial_query_id = roots.query_id
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND event_time >= (
    SELECT min(query_start_time) FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment IN ('05218_codec_none', '05218_codec_lz4', '05218_codec_zstd', '05218_codec_sink_none', '05218_codec_sink_lz4', '05218_codec_sink_zstd1', '05218_codec_sink_zstd19')
      AND type = 'QueryFinish')
GROUP BY run, task;

SELECT 'serializers: uncompressed packets are bigger than LZ4 and ZSTD:',
    (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_none') > (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_lz4'),
    (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_none') > (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_zstd');

SELECT 'sorted gather: every task with a sink has a serializer, and uncompressed is bigger than LZ4:',
    (SELECT countIf(sinks > 0 AND serializers = 0) FROM v_exchange_codec) = 0,
    (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_sink_none') > (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_sink_lz4');

-- The tasks run with the codec settings of the initiator, so the level is applied on every sending
-- task: level 19 packs the same packets tighter than level 1.
SELECT 'zstd level: the tasks run with the level of the initiator, and level 19 packs tighter than level 1:',
    (SELECT countIf(Settings['network_zstd_compression_level'] != '1') FROM system.query_log AS task_log INNER JOIN v_exchange_codec AS tasks ON task_log.query_id = tasks.task
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND type = 'QueryFinish' AND tasks.run = '05218_codec_sink_zstd1') = 0,
    (SELECT countIf(Settings['network_zstd_compression_level'] != '19') FROM system.query_log AS task_log INNER JOIN v_exchange_codec AS tasks ON task_log.query_id = tasks.task
     WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND type = 'QueryFinish' AND tasks.run = '05218_codec_sink_zstd19') = 0,
    (SELECT count() FROM v_exchange_codec WHERE run = '05218_codec_sink_zstd19') > 0,
    (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_sink_zstd1') > (SELECT sum(serializer_bytes) FROM v_exchange_codec WHERE run = '05218_codec_sink_zstd19');

DROP VIEW v_exchange_codec;
DROP TABLE t_exchange_codec;
