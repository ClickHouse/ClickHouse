SET allow_projection_column_list_in_replicated_metadata = 1;
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_projection_dynamic_default;
DROP TABLE IF EXISTS t_projection_dynamic_default_copy;

-- The part default can be NONE, an encryption codec, a configured choice, or a
-- TTL recompression codec. No single factory default validates a mixed chain.
CREATE TABLE t_projection_dynamic_default
(k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Default)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'NONE'; -- { serverError BAD_ARGUMENTS }
-- The same declaration must fail before an ON CLUSTER entry is enqueued.
CREATE TABLE t_projection_dynamic_default ON CLUSTER projection_codec_missing_cluster_05276
(k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Default)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'NONE'; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_dynamic_default';

SET allow_suspicious_codecs = 1;
CREATE TABLE t_projection_dynamic_default
(k UInt64, x UInt64, PROJECTION p (x CODEC(Default, LZ4)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
SET allow_suspicious_codecs = 0;

-- A lone Default has the same behavior as an omitted projection column codec.
CREATE TABLE t_projection_dynamic_default
(k UInt64, x UInt64, PROJECTION p (x CODEC(Default)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'NONE';
INSERT INTO t_projection_dynamic_default VALUES (1, 1);
SELECT default_compression_codec FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_projection_dynamic_default' AND name = 'p' AND active;

ALTER TABLE t_projection_dynamic_default ADD PROJECTION q
(x CODEC(Delta, Default)) AS (SELECT k, x ORDER BY k); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_projection_dynamic_default ON CLUSTER projection_codec_missing_cluster_05276 ADD PROJECTION q
(x CODEC(Delta, Default)) AS (SELECT k, x ORDER BY k); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_dynamic_default' AND name = 'q';

CREATE TABLE t_projection_dynamic_default_copy AS t_projection_dynamic_default
ENGINE = MergeTree ORDER BY k SETTINGS default_compression_codec = 'NONE';
SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_dynamic_default_copy' AND name = 'p';
DROP TABLE t_projection_dynamic_default_copy;
DROP TABLE t_projection_dynamic_default;
