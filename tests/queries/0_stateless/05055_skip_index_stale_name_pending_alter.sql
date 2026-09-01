-- A pending DROP or RENAME must disable a part's stale skip index stored under the affected
-- column name: the index still describes the dropped or renamed-away data, while reads already
-- return the re-added column's default or the renamed column's values.

DROP TABLE IF EXISTS skip_pending;

CREATE TABLE skip_pending (x Int64, rx Int64, ry Int64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 1;

SYSTEM STOP MERGES skip_pending; -- keep the alter mutation pending
INSERT INTO skip_pending VALUES (1, 1, 100), (2, 2, 200);

ALTER TABLE skip_pending
    DROP COLUMN x, ADD COLUMN x Int64 DEFAULT 7,
    DROP COLUMN rx, RENAME COLUMN ry TO rx
SETTINGS alter_sync = 0, mutations_sync = 0;

-- Reads already see the re-added default and the renamed column's data.
SELECT x FROM skip_pending ORDER BY x;
SELECT rx FROM skip_pending ORDER BY rx;

-- The part's auto_minmax_index_x still holds [1, 2] and must not prune the part.
SELECT count() FROM skip_pending WHERE x = 7;
SELECT count() FROM skip_pending WHERE x = 7 SETTINGS use_skip_indexes = 0;

-- Same for auto_minmax_index_rx: it belongs to the dropped column, not to the renamed one.
SELECT count() FROM skip_pending WHERE rx = 100;
SELECT count() FROM skip_pending WHERE rx = 200;

DROP TABLE skip_pending;
