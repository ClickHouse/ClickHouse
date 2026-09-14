-- Regression: compact/non-wide mutations must DROP/CLEAR a column under the name the
-- part still stores after a pending or same-statement `RENAME COLUMN`. The wide/full
-- path already remaps via `nameInPart`; without the same lookup the rename-map replay
-- re-reads the old values into the re-added or cleared column.

DROP TABLE IF EXISTS compact_readd_after_rename;
CREATE TABLE compact_readd_after_rename (a UInt64, k UInt64)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO compact_readd_after_rename VALUES (10, 1);

SELECT 'same-statement part type', part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'compact_readd_after_rename' AND active;

ALTER TABLE compact_readd_after_rename
    (RENAME COLUMN a TO b), (DROP COLUMN b), (ADD COLUMN IF NOT EXISTS b UInt64 DEFAULT 7);
SELECT 'compact drop and re-add after rename', k, b FROM compact_readd_after_rename;
CHECK TABLE compact_readd_after_rename;
DROP TABLE compact_readd_after_rename;

DROP TABLE IF EXISTS compact_clear_after_rename;
CREATE TABLE compact_clear_after_rename (a UInt64, k UInt64)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO compact_clear_after_rename VALUES (10, 1);

ALTER TABLE compact_clear_after_rename (RENAME COLUMN a TO b), (CLEAR COLUMN b);
SELECT 'compact rename and clear', k, b FROM compact_clear_after_rename;
CHECK TABLE compact_clear_after_rename;
DROP TABLE compact_clear_after_rename;

-- The part stays behind an earlier rename mutation; the later DROP/CLEAR must still
-- discard the source-part column before the rename-map replay.
DROP TABLE IF EXISTS compact_readd_after_pending_rename;
CREATE TABLE compact_readd_after_pending_rename (a UInt64, k UInt64)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO compact_readd_after_pending_rename VALUES (10, 1);

SELECT 'pending part type', part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'compact_readd_after_pending_rename' AND active;

ALTER TABLE compact_readd_after_pending_rename RENAME COLUMN a TO b
    SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE compact_readd_after_pending_rename
    (DROP COLUMN b), (ADD COLUMN IF NOT EXISTS b UInt64 DEFAULT 7)
    SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE compact_readd_after_pending_rename UPDATE k = k WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'compact drop and re-add after pending rename', k, b FROM compact_readd_after_pending_rename;
CHECK TABLE compact_readd_after_pending_rename;
DROP TABLE compact_readd_after_pending_rename;

DROP TABLE IF EXISTS compact_clear_after_pending_rename;
CREATE TABLE compact_clear_after_pending_rename (a UInt64, k UInt64)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO compact_clear_after_pending_rename VALUES (10, 1);

ALTER TABLE compact_clear_after_pending_rename RENAME COLUMN a TO b
    SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE compact_clear_after_pending_rename CLEAR COLUMN b
    SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE compact_clear_after_pending_rename UPDATE k = k WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'compact clear after pending rename', k, b FROM compact_clear_after_pending_rename;
CHECK TABLE compact_clear_after_pending_rename;
DROP TABLE compact_clear_after_pending_rename;
