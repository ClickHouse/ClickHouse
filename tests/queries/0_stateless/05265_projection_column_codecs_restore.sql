DROP TABLE IF EXISTS t_projection_codec_restore;

SET allow_suspicious_codecs = 1;
CREATE TABLE t_projection_codec_restore
    (x UInt64, PROJECTION p (x CODEC(Gorilla)) AS (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x;
BACKUP TABLE t_projection_codec_restore TO Memory('05265_projection_codec_restore') FORMAT Null;
DROP TABLE t_projection_codec_restore SYNC;

-- RESTORE introduces a definition from a backup and rechecks its projection codec against
-- the restoring session. An accepted stored ATTACH still skips that session-dependent check.
SET allow_suspicious_codecs = 0;
RESTORE TABLE t_projection_codec_restore FROM Memory('05265_projection_codec_restore') FORMAT Null; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_restore';

SET allow_suspicious_codecs = 1;
RESTORE TABLE t_projection_codec_restore FROM Memory('05265_projection_codec_restore') FORMAT Null;
SELECT codecs FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_codec_restore';
DROP TABLE t_projection_codec_restore;
