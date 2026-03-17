-- Test 1: serialization.json is keyed by physical name, not logical name
DROP TABLE IF EXISTS t_phys_ser_json;

CREATE TABLE t_phys_ser_json
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

ALTER TABLE t_phys_ser_json ADD COLUMN c String;

-- Insert enough data to trigger serialization hints
INSERT INTO t_phys_ser_json SELECT number, toString(number), toString(number) FROM numbers(1000);

-- physical_name column should show 'c' maps to a counter-allocated name
SELECT column, physical_name
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_phys_ser_json' AND active AND column = 'c'
LIMIT 1;

DROP TABLE t_phys_ser_json;

-- Test 2: columns.txt uses logical names (backward compat)
-- After metadata-only rename, part-level columns.txt still has old name
DROP TABLE IF EXISTS t_phys_colnames;

CREATE TABLE t_phys_colnames
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names';

INSERT INTO t_phys_colnames VALUES (1, 'one');
ALTER TABLE t_phys_colnames RENAME COLUMN b TO d;

-- Part-level column is still 'b' (metadata-only rename doesn't rewrite parts)
SELECT column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_phys_colnames' AND active AND column IN ('b', 'd')
ORDER BY column;

DROP TABLE t_phys_colnames;

-- Test 3: Setting disabled prevents activation
DROP TABLE IF EXISTS t_phys_no_activate;

CREATE TABLE t_phys_no_activate
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    activate_physical_names_for_existing_tables = 0;

INSERT INTO t_phys_no_activate VALUES (1, 'one');

-- ADD COLUMN should NOT activate physical names
ALTER TABLE t_phys_no_activate ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_phys_no_activate (a, b, c) VALUES (2, 'two', 22);

-- RENAME should produce a mutation (physical names not active)
ALTER TABLE t_phys_no_activate RENAME COLUMN b TO d;
SELECT count() >= 1 FROM system.mutations WHERE database = currentDatabase() AND table = 't_phys_no_activate';

DROP TABLE t_phys_no_activate;

-- Test 4: First activating RENAME is metadata-only
DROP TABLE IF EXISTS t_phys_first_rename;

CREATE TABLE t_phys_first_rename
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0;

INSERT INTO t_phys_first_rename VALUES (1, 'one');

ALTER TABLE t_phys_first_rename MODIFY SETTING
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

-- The first RENAME activates physical names AND is metadata-only (no mutation)
ALTER TABLE t_phys_first_rename RENAME COLUMN b TO d;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_phys_first_rename' AND NOT is_done;
SELECT a, d FROM t_phys_first_rename ORDER BY a;

DROP TABLE t_phys_first_rename;

-- Test 5: First activating DROP is metadata-only
DROP TABLE IF EXISTS t_phys_first_drop;

CREATE TABLE t_phys_first_drop
(
    a UInt64,
    b String,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0;

INSERT INTO t_phys_first_drop VALUES (1, 'one', 10);

ALTER TABLE t_phys_first_drop MODIFY SETTING
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

-- The first DROP activates physical names AND is metadata-only
ALTER TABLE t_phys_first_drop DROP COLUMN c;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_phys_first_drop' AND NOT is_done;
SELECT * FROM t_phys_first_drop ORDER BY a;

DROP TABLE t_phys_first_drop;

-- Test 6: RENAME non-key column on partitioned table, verify partition pruning and minmax
DROP TABLE IF EXISTS t_phys_rename_pk;

CREATE TABLE t_phys_rename_pk
(
    a UInt64,
    b String
)
ENGINE = MergeTree
PARTITION BY a
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_rename_pk VALUES (1, 'one');
INSERT INTO t_phys_rename_pk VALUES (2, 'two');
INSERT INTO t_phys_rename_pk VALUES (3, 'three');

-- Rename non-key column
ALTER TABLE t_phys_rename_pk RENAME COLUMN b TO d;

-- Verify reads work across rename
SELECT a, d FROM t_phys_rename_pk ORDER BY a;

-- Verify partition pruning still works
SELECT a, d FROM t_phys_rename_pk WHERE a = 2;

-- Insert after rename
INSERT INTO t_phys_rename_pk VALUES (4, 'four');
SELECT a, d FROM t_phys_rename_pk WHERE a >= 3 ORDER BY a;

DROP TABLE t_phys_rename_pk;

-- Test 7: Mutation on compact physical-name table
DROP TABLE IF EXISTS t_phys_compact_mut;

CREATE TABLE t_phys_compact_mut
(
    a UInt64,
    b UInt32
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_compact_mut VALUES (1, 10);
INSERT INTO t_phys_compact_mut VALUES (2, 20);

ALTER TABLE t_phys_compact_mut ADD COLUMN c String DEFAULT 'x';
INSERT INTO t_phys_compact_mut VALUES (3, 30, 'y');

-- MODIFY COLUMN: mutation on compact parts with physical names
ALTER TABLE t_phys_compact_mut MODIFY COLUMN b UInt64;
SELECT a, b, toTypeName(b), c FROM t_phys_compact_mut ORDER BY a;

DROP TABLE t_phys_compact_mut;

-- Test 8: Projection merge across rename boundary
DROP TABLE IF EXISTS t_phys_proj_merge;

CREATE TABLE t_phys_proj_merge
(
    a UInt64,
    b String,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

ALTER TABLE t_phys_proj_merge ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);

-- Insert before rename
INSERT INTO t_phys_proj_merge VALUES (1, 'one', 10);
INSERT INTO t_phys_proj_merge VALUES (1, 'two', 20);

-- Rename
ALTER TABLE t_phys_proj_merge RENAME COLUMN b TO d;

-- Insert after rename
INSERT INTO t_phys_proj_merge VALUES (2, 'three', 30);
INSERT INTO t_phys_proj_merge VALUES (2, 'four', 40);

-- Merge pre-rename and post-rename parts
OPTIMIZE TABLE t_phys_proj_merge FINAL;

-- Projection should still work after merging parts from both sides of the rename
SELECT a, sum(c) FROM t_phys_proj_merge GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;

DROP TABLE t_phys_proj_merge;

-- Test 9: system.parts_columns exposes physical_name
DROP TABLE IF EXISTS t_phys_sysparts;

CREATE TABLE t_phys_sysparts
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

ALTER TABLE t_phys_sysparts ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_phys_sysparts VALUES (1, 'one', 10);

-- 'a' and 'b' should have physical_name == column name (identity mapping)
-- 'c' should have a counter-allocated physical name ('1')
SELECT column, physical_name
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_phys_sysparts' AND active
ORDER BY column;

-- After rename, part-level column names remain unchanged (metadata-only rename)
ALTER TABLE t_phys_sysparts RENAME COLUMN b TO d;

-- Part still shows 'b' at part level, physical_name unchanged
SELECT column, physical_name
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_phys_sysparts' AND active
ORDER BY column;

DROP TABLE t_phys_sysparts;

-- Test 10: Merge compact + wide parts
DROP TABLE IF EXISTS t_phys_mixed_parts;

CREATE TABLE t_phys_mixed_parts
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 100,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

-- Small insert -> compact part
INSERT INTO t_phys_mixed_parts VALUES (1, 'x');

-- Large insert -> wide part
INSERT INTO t_phys_mixed_parts SELECT number, repeat('y', 200) FROM numbers(100, 50);

ALTER TABLE t_phys_mixed_parts ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_phys_mixed_parts VALUES (200, 'z', 42);

OPTIMIZE TABLE t_phys_mixed_parts FINAL;
SELECT count(), sum(c) FROM t_phys_mixed_parts;

DROP TABLE t_phys_mixed_parts;

-- Test 11: RENAME column used in skip index
DROP TABLE IF EXISTS t_phys_skip_idx;

CREATE TABLE t_phys_skip_idx
(
    a UInt64,
    b String,
    INDEX idx_b b TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_skip_idx VALUES (1, 'hello world');
INSERT INTO t_phys_skip_idx VALUES (2, 'foo bar');

ALTER TABLE t_phys_skip_idx RENAME COLUMN b TO d;

SELECT a, d FROM t_phys_skip_idx ORDER BY a;

INSERT INTO t_phys_skip_idx VALUES (3, 'baz qux');
OPTIMIZE TABLE t_phys_skip_idx FINAL;
SELECT a, d FROM t_phys_skip_idx ORDER BY a;

DROP TABLE t_phys_skip_idx;

-- Test 12: system.projection_parts_columns exposes physical_name
DROP TABLE IF EXISTS t_phys_proj_sys;

CREATE TABLE t_phys_proj_sys
(
    a UInt64,
    b String,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

ALTER TABLE t_phys_proj_sys ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);
INSERT INTO t_phys_proj_sys VALUES (1, 'one', 10);

SELECT column, physical_name
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_phys_proj_sys' AND active
ORDER BY column;

DROP TABLE t_phys_proj_sys;
