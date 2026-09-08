-- Regression: a `DROP`/`CLEAR COLUMN` of a `Nested` parent on a wide part must remove the
-- flattened member files (`n.x`, `n.y`, ...) even when a member was renamed by a pending
-- rename of the same mutation. Previously the rename carried the old member files into the
-- new part under the new name, so `CLEAR COLUMN n` silently kept the stale data of the
-- renamed member (and the files leaked for a plain `DROP COLUMN n`).
-- The table settings pin the wide-part layout so the hardlink fast path is exercised
-- deterministically (see 05076_alter_drop_nested_parent_wide_part).

DROP TABLE IF EXISTS clear_nested_after_rename;

CREATE TABLE clear_nested_after_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO clear_nested_after_rename VALUES (1, [10], [20]), (2, [11], [21]);

SELECT 'part type', part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'clear_nested_after_rename' AND active;

-- Rename a member and clear the parent by the same mutation.
ALTER TABLE clear_nested_after_rename (RENAME COLUMN n.x TO n.z), (CLEAR COLUMN n)
SETTINGS mutations_sync = 2;

SELECT 'rename and clear in one alter', a, n.z, n.y FROM clear_nested_after_rename ORDER BY a;
CHECK TABLE clear_nested_after_rename;
DROP TABLE clear_nested_after_rename;

-- The reviewer's scenario: the parent is cleared while the part is still behind the
-- earlier rename mutation. Mutations are applied in version order, so a following
-- synchronous rewrite waits for the rename and the clear to be applied.
CREATE TABLE clear_nested_after_pending_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO clear_nested_after_pending_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE clear_nested_after_pending_rename RENAME COLUMN n.x TO n.z SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE clear_nested_after_pending_rename CLEAR COLUMN n SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE clear_nested_after_pending_rename UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'clear after pending rename', a, n.z, n.y FROM clear_nested_after_pending_rename ORDER BY a;
CHECK TABLE clear_nested_after_pending_rename;
DROP TABLE clear_nested_after_pending_rename;

-- The rename mutation is given time to mutate the part first: the clear then runs against
-- a part that already stores `n.z`.
CREATE TABLE clear_nested_after_applied_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO clear_nested_after_applied_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE clear_nested_after_applied_rename RENAME COLUMN n.x TO n.z SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE clear_nested_after_applied_rename UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE clear_nested_after_applied_rename CLEAR COLUMN n SETTINGS mutations_sync = 2;

SELECT 'clear after applied rename', a, n.z, n.y FROM clear_nested_after_applied_rename ORDER BY a;
CHECK TABLE clear_nested_after_applied_rename;
DROP TABLE clear_nested_after_applied_rename;

-- Dropping the parent after a pending member rename must remove the renamed member's
-- files as well: a same-name re-add of the old member must not resurrect the data.
CREATE TABLE drop_nested_after_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO drop_nested_after_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE drop_nested_after_rename (RENAME COLUMN n.x TO n.z), (DROP COLUMN n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64))
SETTINGS mutations_sync = 2;

SELECT 'drop and re-add after rename', a, n.x FROM drop_nested_after_rename ORDER BY a;
CHECK TABLE drop_nested_after_rename;
DROP TABLE drop_nested_after_rename;
