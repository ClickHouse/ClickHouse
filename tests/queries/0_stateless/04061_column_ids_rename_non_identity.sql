-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression test: renaming a column that was added AFTER activation
-- (non-identity column ID, e.g. "0") must keep old parts readable.
-- Before the fix, columns.txt stored logical names; after rename the
-- old logical name was absent from the mapping and the part was broken.

SET allow_experimental_column_ids = 1;

SELECT 'Test: rename non-identity column after insert';

DROP TABLE IF EXISTS t_rename_non_identity;

CREATE TABLE t_rename_non_identity
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;

-- Column `a` gets identity-mapped column ID "a".
INSERT INTO t_rename_non_identity VALUES (1);

-- Add a new column; it gets a numeric column ID (e.g. "0").
ALTER TABLE t_rename_non_identity ADD COLUMN b String DEFAULT 'dflt';
INSERT INTO t_rename_non_identity (a, b) VALUES (2, 'hello');

-- Rename the non-identity column.  This is metadata-only; old parts
-- keep their columns.txt unchanged.  The fix writes column IDs
-- (not logical names) to columns.txt, so the old part stays resolvable.
ALTER TABLE t_rename_non_identity RENAME COLUMN b TO c;

-- Verify: all rows readable, including the part written before rename.
SELECT a, c FROM t_rename_non_identity ORDER BY a;

-- Insert after rename and merge all parts together.
INSERT INTO t_rename_non_identity (a, c) VALUES (3, 'world');
OPTIMIZE TABLE t_rename_non_identity FINAL;
SELECT a, c FROM t_rename_non_identity ORDER BY a;

DROP TABLE t_rename_non_identity;
