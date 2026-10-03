DROP TABLE IF EXISTS t_ttl_untyped_codec_source;
DROP TABLE IF EXISTS t_ttl_untyped_codec_fresh;
DROP TABLE IF EXISTS t_ttl_untyped_codec_copy;

CREATE TABLE t_ttl_untyped_codec_source (dt DateTime, k UInt64, x String CODEC(NONE))
ENGINE = MergeTree ORDER BY k TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(LZ4);

-- A RECOMPRESS codec also compresses part statistics, which have no value type.
CREATE TABLE t_ttl_untyped_codec_fresh (dt DateTime, k UInt64, x String CODEC(NONE))
ENGINE = MergeTree ORDER BY k TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(T64); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_untyped_codec_fresh';

ALTER TABLE t_ttl_untyped_codec_source
MODIFY TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(T64); -- { serverError BAD_ARGUMENTS }
SELECT position(create_table_query, 'RECOMPRESS CODEC(T64)') = 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_ttl_untyped_codec_source';

CREATE TABLE t_ttl_untyped_codec_copy AS t_ttl_untyped_codec_source
ENGINE = MergeTree ORDER BY k TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(T64); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_untyped_codec_copy';

INSERT INTO t_ttl_untyped_codec_source VALUES (now() - INTERVAL 1 DAY, 1, 'a');
INSERT INTO t_ttl_untyped_codec_source VALUES (now() - INTERVAL 1 DAY, 2, 'b');
OPTIMIZE TABLE t_ttl_untyped_codec_source FINAL;
SELECT x FROM t_ttl_untyped_codec_source ORDER BY k;

DROP TABLE t_ttl_untyped_codec_source;
