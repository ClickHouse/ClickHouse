SET allow_projection_column_list_in_replicated_metadata = 1;
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_projection_default_codec_source;
DROP TABLE IF EXISTS t_projection_default_codec_fresh;
DROP TABLE IF EXISTS t_projection_default_codec_plain;
DROP TABLE IF EXISTS t_projection_default_codec_copy;

CREATE TABLE t_projection_default_codec_source
(
    k UInt64,
    x Float64 CODEC(NONE),
    PROJECTION p (x CODEC(Default)) AS (SELECT k, x ORDER BY k)
) ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'LZ4';

-- T64 can be constructed with no type but cannot compress part statistics,
-- which use the default codec as an untyped byte stream.
CREATE TABLE t_projection_default_codec_fresh
(k UInt64, x Float64 CODEC(NONE), PROJECTION p (x CODEC(Default)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_fresh';

CREATE TABLE t_projection_default_codec_plain (k UInt64 CODEC(NONE), x Float64 CODEC(NONE))
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_plain';

ALTER TABLE t_projection_default_codec_source MODIFY SETTING default_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
SELECT position(create_table_query, 'T64') = 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_default_codec_source';

CREATE TABLE t_projection_default_codec_copy AS t_projection_default_codec_source
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'T64'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_default_codec_copy';

INSERT INTO t_projection_default_codec_source (k, x) VALUES (1, 1.125);
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_projection_default_codec_source' AND name = 'p' AND active;
SELECT x FROM t_projection_default_codec_source ORDER BY k;

DROP TABLE t_projection_default_codec_source;
