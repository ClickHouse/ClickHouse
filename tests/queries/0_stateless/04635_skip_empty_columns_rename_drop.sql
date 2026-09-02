-- Regression: a pending RENAME COLUMN followed by DROP COLUMN records the drop
-- under the renamed name, while the missing-columns marker is recorded under the
-- old physical name. The reader must detect the drop through the rename mapping,
-- otherwise a column re-added after the drop reads the stale frozen default of
-- the skipped column instead of its own DEFAULT expression.

SET mutations_sync = 0, alter_sync = 0;

DROP TABLE IF EXISTS t_skip_empty_rename_drop;

CREATE TABLE t_skip_empty_rename_drop
(
    key UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

-- b=0 (type-default) -> b is skipped and gets a missing-columns marker.
INSERT INTO t_skip_empty_rename_drop (key, a, b) VALUES (1, 100, 0);

-- Keep the mutations pending so the read goes through on-the-fly alter
-- conversions: the rename map contains c <- b and the drop set contains c.
-- The three commands must be issued as a single ALTER: since #112783 a
-- separate metadata ALTER on plain MergeTree waits for a pending barrier
-- (rename) mutation to finish, so sequential ALTERs would block under
-- SYSTEM STOP MERGES and the pending state could never be observed.
SYSTEM STOP MERGES t_skip_empty_rename_drop;

ALTER TABLE t_skip_empty_rename_drop RENAME COLUMN b TO c, DROP COLUMN c, ADD COLUMN c UInt64 DEFAULT 999;

-- The re-added c must read its DEFAULT (999), not the frozen type-default (0)
-- of the dropped, formerly skipped b.
SELECT 'rename_drop_add_pending';
SELECT key, a, c FROM t_skip_empty_rename_drop ORDER BY key;

SYSTEM START MERGES t_skip_empty_rename_drop;

-- After the pending mutations materialize, the result must stay the same.
SET mutations_sync = 2;
ALTER TABLE t_skip_empty_rename_drop UPDATE a = a WHERE 1;

SELECT 'rename_drop_add_materialized';
SELECT key, a, c FROM t_skip_empty_rename_drop ORDER BY key;

DROP TABLE t_skip_empty_rename_drop;

-- Same without the rename: DROP of a marker-only column followed by ADD COLUMN
-- of the same name in one materialization window must also forget the marker.

DROP TABLE IF EXISTS t_skip_empty_drop_add;

CREATE TABLE t_skip_empty_drop_add
(
    key UInt64,
    a UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 1.0,
         skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         enable_block_number_column = 0, enable_block_offset_column = 0;

SET mutations_sync = 0, alter_sync = 0;

INSERT INTO t_skip_empty_drop_add (key, a, b) VALUES (1, 100, 0);

SYSTEM STOP MERGES t_skip_empty_drop_add;

ALTER TABLE t_skip_empty_drop_add DROP COLUMN b;
ALTER TABLE t_skip_empty_drop_add ADD COLUMN b UInt64 DEFAULT 999;

SELECT 'drop_add_pending';
SELECT key, a, b FROM t_skip_empty_drop_add ORDER BY key;

SYSTEM START MERGES t_skip_empty_drop_add;

SET mutations_sync = 2;
ALTER TABLE t_skip_empty_drop_add UPDATE a = a WHERE 1;

SELECT 'drop_add_materialized';
SELECT key, a, b FROM t_skip_empty_drop_add ORDER BY key;

DROP TABLE t_skip_empty_drop_add;
