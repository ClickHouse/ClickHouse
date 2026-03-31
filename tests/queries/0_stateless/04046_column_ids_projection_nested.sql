-- Tags: no-random-settings, no-random-merge-tree-settings
-- Test 1: Projection parts survive rename and reload
SET allow_experimental_column_ids = 1;

SELECT 'Test 1: projection parts survive rename and reload';
DROP TABLE IF EXISTS t_phys_proj;

CREATE TABLE t_phys_proj
(
    a UInt64,
    b String,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;

ALTER TABLE t_phys_proj ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);

INSERT INTO t_phys_proj VALUES (1, 'one', 10);
INSERT INTO t_phys_proj VALUES (1, 'two', 20);
INSERT INTO t_phys_proj VALUES (2, 'three', 30);

SELECT a, sum(c) FROM t_phys_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

ALTER TABLE t_phys_proj RENAME COLUMN b TO d;

SELECT a, sum(c) FROM t_phys_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

OPTIMIZE TABLE t_phys_proj FINAL;

SELECT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_proj' AND active AND NOT startsWith(column, '_') ORDER BY column;

SELECT a, sum(c) FROM t_phys_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

DROP TABLE t_phys_proj;

-- Test 2: ADD flattened Nested with column IDs active (compound column IDs)
SELECT 'Test 2: ADD flattened Nested with column IDs';
DROP TABLE IF EXISTS t_phys_flat_add;
CREATE TABLE t_phys_flat_add (a UInt64, b String) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;

INSERT INTO t_phys_flat_add VALUES (1, 'one');

ALTER TABLE t_phys_flat_add ADD COLUMN n Nested(x UInt64, y String);

INSERT INTO t_phys_flat_add VALUES (2, 'two', [10, 20], ['a', 'b']);
INSERT INTO t_phys_flat_add VALUES (3, 'three', [30], ['c']);

SELECT a, b, `n.x`, `n.y` FROM t_phys_flat_add ORDER BY a;

-- Column IDs: siblings share a compound prefix (e.g. "3.x", "3.y")
SELECT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_phys_flat_add' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;

OPTIMIZE TABLE t_phys_flat_add FINAL;
SELECT a, b, `n.x`, `n.y` FROM t_phys_flat_add ORDER BY a;

DROP TABLE t_phys_flat_add;

-- Test 3: RENAME field within flattened Nested group (metadata-only)
SELECT 'Test 3: RENAME field within Nested group';
DROP TABLE IF EXISTS t_phys_flat_rename;
CREATE TABLE t_phys_flat_rename (a UInt64, b String) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;

ALTER TABLE t_phys_flat_rename ADD COLUMN n Nested(x UInt64, y String);

INSERT INTO t_phys_flat_rename VALUES (1, 'hello', [10, 20], ['a', 'b']);
INSERT INTO t_phys_flat_rename VALUES (2, 'world', [30], ['c']);

-- Rename the field name (n.x -> n.z): stays metadata-only
ALTER TABLE t_phys_flat_rename RENAME COLUMN `n.x` TO `n.z`;

SELECT a, b, `n.z`, `n.y` FROM t_phys_flat_rename ORDER BY a;

OPTIMIZE TABLE t_phys_flat_rename FINAL;
SELECT a, b, `n.z`, `n.y` FROM t_phys_flat_rename ORDER BY a;

-- Column ID is unchanged (still the compound form)
SELECT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_phys_flat_rename' AND active
    AND column LIKE 'n.%'
    ORDER BY column, column_id;

DROP TABLE t_phys_flat_rename;

-- Test 4: Existing flattened Nested survives activation + rename (identity mapping)
SELECT 'Test 4: existing flattened Nested with identity mapping';
DROP TABLE IF EXISTS t_phys_flat_existing;
CREATE TABLE t_phys_flat_existing (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_phys_flat_existing VALUES (1, [10, 20], ['a', 'b']);
INSERT INTO t_phys_flat_existing VALUES (2, [30], ['c']);

-- Activate column IDs on existing table
ALTER TABLE t_phys_flat_existing MODIFY SETTING
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;

-- Add a new column to trigger activation
ALTER TABLE t_phys_flat_existing ADD COLUMN c UInt64 DEFAULT 0;

-- Identity mapping: n.x -> n.x, n.y -> n.y
SELECT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_phys_flat_existing' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;

-- Rename a non-nested column (metadata-only, no impact on offsets)
ALTER TABLE t_phys_flat_existing RENAME COLUMN c TO d;

SELECT a, `n.x`, `n.y`, d FROM t_phys_flat_existing ORDER BY a;

OPTIMIZE TABLE t_phys_flat_existing FINAL;
SELECT a, `n.x`, `n.y`, d FROM t_phys_flat_existing ORDER BY a;

DROP TABLE t_phys_flat_existing;

-- Test 5: Non-flattened Nested with column IDs (flatten_nested = 0)
SELECT 'Test 5: non-flattened Nested with column IDs';
SET flatten_nested = 0;
DROP TABLE IF EXISTS t_phys_nested_nf;

CREATE TABLE t_phys_nested_nf
(
    a UInt64,
    b String,
    n Nested(x UInt64, y String)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;

INSERT INTO t_phys_nested_nf VALUES (1, 'hello', [(10, 'a'), (20, 'b')]);
INSERT INTO t_phys_nested_nf VALUES (2, 'world', [(30, 'c')]);

ALTER TABLE t_phys_nested_nf RENAME COLUMN b TO d;

SELECT a, d, n.x, n.y FROM t_phys_nested_nf ORDER BY a;

OPTIMIZE TABLE t_phys_nested_nf FINAL;
SELECT a, d, n.x, n.y FROM t_phys_nested_nf ORDER BY a;

DROP TABLE t_phys_nested_nf;
SET flatten_nested = 1;

-- Test 6: Identity-mapped Nested parent rename is metadata-only
-- Regression: identity-mapped Nested columns (physical == logical, e.g.
-- "n.x" -> "n.x") can be renamed across Nested parents as metadata-only
-- because getFileNameForStreamByColumnId derives the offset stream name from
-- the physical prefix ("n"), not the logical prefix, so the shared offset
-- stream "n.size0" remains correct after the rename.
SELECT 'Test 6: identity-mapped Nested parent rename is metadata-only';
DROP TABLE IF EXISTS t_phys_identity_nested_rename;
CREATE TABLE t_phys_identity_nested_rename (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_phys_identity_nested_rename VALUES (1, [10, 20], ['a', 'b']);
INSERT INTO t_phys_identity_nested_rename VALUES (2, [30], ['c']);

ALTER TABLE t_phys_identity_nested_rename MODIFY SETTING
    serialization_info_version = 'with_column_ids',
    activate_column_ids_for_existing_tables = 1;
ALTER TABLE t_phys_identity_nested_rename ADD COLUMN c UInt64 DEFAULT 0;

SELECT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_phys_identity_nested_rename' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;

ALTER TABLE t_phys_identity_nested_rename RENAME COLUMN `n.x` TO `m.x`, RENAME COLUMN `n.y` TO `m.y`;

SELECT a, `m.x`, `m.y` FROM t_phys_identity_nested_rename ORDER BY a;
OPTIMIZE TABLE t_phys_identity_nested_rename FINAL;
SELECT a, `m.x`, `m.y` FROM t_phys_identity_nested_rename ORDER BY a;

DROP TABLE t_phys_identity_nested_rename;
