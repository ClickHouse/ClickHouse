-- A pending alter mutation is applied to the parent read on the fly (via `AlterConversions`), but not to a
-- `mergeTreeProjection` read, which clears the mutations snapshot. Under a row policy the projection could
-- therefore show stale values, so the read is refused, as for pending data mutations and patch parts.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_proj_rls_alter;
DROP ROW POLICY IF EXISTS rp_proj_rls_alter ON t_proj_rls_alter;

CREATE TABLE t_proj_rls_alter (id UInt64, v UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_proj_rls_alter VALUES (1, 1), (2, 1), (3, 0);

ALTER TABLE t_proj_rls_alter ADD PROJECTION p (SELECT id, v ORDER BY id);
ALTER TABLE t_proj_rls_alter MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

CREATE ROW POLICY rp_proj_rls_alter ON t_proj_rls_alter FOR SELECT USING v = 1 TO ALL;

SELECT '-- no pending mutation';
SELECT id FROM mergeTreeProjection(currentDatabase(), 't_proj_rls_alter', 'p') ORDER BY id;

SYSTEM STOP MERGES t_proj_rls_alter;
ALTER TABLE t_proj_rls_alter MODIFY COLUMN v UInt16 SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT '-- pending alter mutation: read is refused';
SELECT id FROM mergeTreeProjection(currentDatabase(), 't_proj_rls_alter', 'p') ORDER BY id; -- { serverError ACCESS_DENIED }

SELECT '-- the parent table is still readable';
SELECT id FROM t_proj_rls_alter ORDER BY id;

SYSTEM START MERGES t_proj_rls_alter;
ALTER TABLE t_proj_rls_alter MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

SELECT '-- after the mutation is done';
SELECT id FROM mergeTreeProjection(currentDatabase(), 't_proj_rls_alter', 'p') ORDER BY id;

DROP ROW POLICY rp_proj_rls_alter ON t_proj_rls_alter;
DROP TABLE t_proj_rls_alter;
