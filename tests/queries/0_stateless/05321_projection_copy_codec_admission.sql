DROP TABLE IF EXISTS t_05321_source_base;
DROP TABLE IF EXISTS t_05321_source_projection;
DROP TABLE IF EXISTS t_05321_direct_base;
DROP TABLE IF EXISTS t_05321_direct_projection;
DROP TABLE IF EXISTS t_05321_copy_base;
DROP TABLE IF EXISTS t_05321_copy_projection;
DROP TABLE IF EXISTS t_05321_attach_base;
DROP TABLE IF EXISTS t_05321_attach_projection_rejected;
DROP TABLE IF EXISTS t_05321_restore_base;

SET enable_alp_codec = 1;
CREATE TABLE t_05321_source_base
(k UInt64, x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05321_source_projection
(k UInt64, x Float64, PROJECTION p (x CODEC(ALP)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k;
BACKUP TABLE t_05321_source_base TO Memory('05321_projection_copy_codec_admission') FORMAT Null;

SET enable_alp_codec = 0;

-- A source's earlier opt-in does not admit a new destination under this session.
CREATE TABLE t_05321_direct_base
(k UInt64, x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05321_direct_projection
(k UInt64, x Float64, PROJECTION p (x CODEC(ALP)) AS (SELECT k, x ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05321_copy_base AS t_05321_source_base
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_05321_copy_projection AS t_05321_source_projection
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t_05321_attach_base UUID '05321000-0000-4000-8000-000000000001'
(k UInt64, x Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t_05321_attach_projection_rejected UUID '05321000-0000-4000-8000-000000000002'
ENGINE = MergeTree ORDER BY k AS t_05321_source_projection; -- { serverError BAD_ARGUMENTS }
RESTORE TABLE t_05321_source_base AS t_05321_restore_base
FROM Memory('05321_projection_copy_codec_admission') FORMAT Null; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables WHERE database = currentDatabase()
AND name IN ('t_05321_direct_base', 't_05321_direct_projection', 't_05321_copy_base',
             't_05321_copy_projection', 't_05321_attach_base', 't_05321_attach_projection_rejected',
             't_05321_restore_base');

-- Re-enabling the codec admits the copies and restore and allows part writes.
SET enable_alp_codec = 1;
CREATE TABLE t_05321_copy_base AS t_05321_source_base ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05321_copy_projection AS t_05321_source_projection ENGINE = MergeTree ORDER BY k;
RESTORE TABLE t_05321_source_base AS t_05321_restore_base
FROM Memory('05321_projection_copy_codec_admission') FORMAT Null;

INSERT INTO t_05321_copy_base SELECT 1, 1.25;
INSERT INTO t_05321_copy_projection SELECT 1, 2.5;
SELECT x FROM t_05321_copy_base ORDER BY k;
SELECT x FROM t_05321_copy_projection ORDER BY k;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase()
AND table = 't_05321_copy_projection' AND name = 'p' AND active;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_05321_restore_base';

-- Loading stored metadata remains possible without a new admission decision.
SET enable_alp_codec = 0;
DETACH TABLE t_05321_source_projection;
ATTACH TABLE t_05321_source_projection;
SELECT count() FROM system.projections WHERE database = currentDatabase()
AND table = 't_05321_source_projection';

DROP TABLE t_05321_source_base;
DROP TABLE t_05321_source_projection;
DROP TABLE t_05321_copy_base;
DROP TABLE t_05321_copy_projection;
DROP TABLE t_05321_restore_base;
