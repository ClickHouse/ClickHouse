-- Tags: no-random-settings, no-random-merge-tree-settings
-- Test 1: Chained rename b → d → e → f
SET allow_experimental_physical_column_names = 1;

SELECT 'Test 1: chained rename';
DROP TABLE IF EXISTS t_phys_chain;

CREATE TABLE t_phys_chain
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

INSERT INTO t_phys_chain VALUES (1, 'hello');
INSERT INTO t_phys_chain VALUES (2, 'world');

ALTER TABLE t_phys_chain RENAME COLUMN b TO d;
SELECT a, d FROM t_phys_chain ORDER BY a;

ALTER TABLE t_phys_chain RENAME COLUMN d TO e;
SELECT a, e FROM t_phys_chain ORDER BY a;

ALTER TABLE t_phys_chain RENAME COLUMN e TO f;
SELECT a, f FROM t_phys_chain ORDER BY a;

OPTIMIZE TABLE t_phys_chain FINAL;
SELECT a, f FROM t_phys_chain ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_chain' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_chain;

-- Test 2: Multi-operation ALTER (ADD + RENAME + DROP in one statement)
SELECT 'Test 2: multi-operation ALTER';
DROP TABLE IF EXISTS t_phys_multi_op;

CREATE TABLE t_phys_multi_op
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

INSERT INTO t_phys_multi_op VALUES (1, 'one', 10);
INSERT INTO t_phys_multi_op VALUES (2, 'two', 20);

ALTER TABLE t_phys_multi_op
    ADD COLUMN d Float64 DEFAULT 3.14,
    RENAME COLUMN b TO name,
    DROP COLUMN c;

SELECT a, name, d FROM t_phys_multi_op ORDER BY a;

INSERT INTO t_phys_multi_op VALUES (3, 'three', 2.72);
SELECT a, name, d FROM t_phys_multi_op ORDER BY a;

OPTIMIZE TABLE t_phys_multi_op FINAL;
SELECT a, name, d FROM t_phys_multi_op ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_multi_op' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_multi_op;

-- Test 3: RENAME then DROP of same column in separate ALTERs
SELECT 'Test 3: rename, drop, re-add';
DROP TABLE IF EXISTS t_phys_rename_drop;

CREATE TABLE t_phys_rename_drop
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

INSERT INTO t_phys_rename_drop VALUES (1, 'one', 10);

ALTER TABLE t_phys_rename_drop RENAME COLUMN b TO d;
ALTER TABLE t_phys_rename_drop DROP COLUMN d;

SELECT a, c FROM t_phys_rename_drop ORDER BY a;

ALTER TABLE t_phys_rename_drop ADD COLUMN d String DEFAULT 'new';
INSERT INTO t_phys_rename_drop VALUES (2, 20, 'added');
SELECT a, c, d FROM t_phys_rename_drop ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_rename_drop' AND active AND column = 'd' ORDER BY column;

DROP TABLE t_phys_rename_drop;

-- Test 4: Nullable column rename
SELECT 'Test 4: nullable column rename';
DROP TABLE IF EXISTS t_phys_nullable;

CREATE TABLE t_phys_nullable
(
    a UInt64,
    b Nullable(String)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_nullable VALUES (1, 'hello');
INSERT INTO t_phys_nullable VALUES (2, NULL);
INSERT INTO t_phys_nullable VALUES (3, 'world');

ALTER TABLE t_phys_nullable RENAME COLUMN b TO d;
SELECT a, d FROM t_phys_nullable ORDER BY a;

OPTIMIZE TABLE t_phys_nullable FINAL;
SELECT a, d FROM t_phys_nullable ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_nullable' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_nullable;

-- Test 5: Empty table operations (no data parts)
SELECT 'Test 5: empty table operations';
DROP TABLE IF EXISTS t_phys_empty;

CREATE TABLE t_phys_empty
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

ALTER TABLE t_phys_empty RENAME COLUMN b TO d;
ALTER TABLE t_phys_empty ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE t_phys_empty DROP COLUMN c;

INSERT INTO t_phys_empty VALUES (1, 'after_rename');
SELECT a, d FROM t_phys_empty ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_empty' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_empty;

-- Test 6: Counter monotonicity across ADD/DROP cycles
SELECT 'Test 6: counter monotonicity';
DROP TABLE IF EXISTS t_phys_counter;

CREATE TABLE t_phys_counter
(
    a UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

ALTER TABLE t_phys_counter ADD COLUMN c1 UInt64 DEFAULT 0;
ALTER TABLE t_phys_counter ADD COLUMN c2 UInt64 DEFAULT 0;
ALTER TABLE t_phys_counter ADD COLUMN c3 UInt64 DEFAULT 0;

ALTER TABLE t_phys_counter DROP COLUMN c1;
ALTER TABLE t_phys_counter DROP COLUMN c2;
ALTER TABLE t_phys_counter DROP COLUMN c3;

ALTER TABLE t_phys_counter ADD COLUMN d1 UInt64 DEFAULT 0;
ALTER TABLE t_phys_counter ADD COLUMN d2 UInt64 DEFAULT 0;

INSERT INTO t_phys_counter VALUES (1, 10, 20);
SELECT a, d1, d2 FROM t_phys_counter ORDER BY a;

SELECT column, physical_name
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_phys_counter' AND active AND column LIKE 'd%'
ORDER BY column;

DROP TABLE t_phys_counter;

-- Test 7: DETACH/ATTACH partition across rename
SELECT 'Test 7: detach/attach across rename';
DROP TABLE IF EXISTS t_phys_detach;

CREATE TABLE t_phys_detach
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

INSERT INTO t_phys_detach VALUES (1, 'hello');
INSERT INTO t_phys_detach VALUES (2, 'world');

ALTER TABLE t_phys_detach RENAME COLUMN b TO d;

ALTER TABLE t_phys_detach DETACH PARTITION 1;
SELECT a, d FROM t_phys_detach ORDER BY a;

ALTER TABLE t_phys_detach ATTACH PARTITION 1;
SELECT a, d FROM t_phys_detach ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_detach' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_detach;

-- Test 8: Map and Tuple types with physical names
SELECT 'Test 8: complex types with physical names';
DROP TABLE IF EXISTS t_phys_complex_types;

CREATE TABLE t_phys_complex_types
(
    a UInt64,
    b Map(String, UInt64),
    c Tuple(x UInt64, y String)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_complex_types VALUES (1, {'k1': 10, 'k2': 20}, (100, 'hello'));
INSERT INTO t_phys_complex_types VALUES (2, {'k3': 30}, (200, 'world'));

ALTER TABLE t_phys_complex_types ADD COLUMN d String DEFAULT 'extra';
ALTER TABLE t_phys_complex_types RENAME COLUMN d TO e;

SELECT a, b, c, e FROM t_phys_complex_types ORDER BY a;

OPTIMIZE TABLE t_phys_complex_types FINAL;
SELECT a, b, c, e FROM t_phys_complex_types ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_complex_types' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_complex_types;

-- Test 9: OPTIMIZE DEDUPLICATE after rename
SELECT 'Test 9: deduplicate after rename';
DROP TABLE IF EXISTS t_phys_dedup;

CREATE TABLE t_phys_dedup
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

INSERT INTO t_phys_dedup VALUES (1, 'hello');
INSERT INTO t_phys_dedup VALUES (1, 'hello');
INSERT INTO t_phys_dedup VALUES (2, 'world');

ALTER TABLE t_phys_dedup RENAME COLUMN b TO d;

OPTIMIZE TABLE t_phys_dedup FINAL DEDUPLICATE BY a, d;
SELECT a, d FROM t_phys_dedup ORDER BY a;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_dedup' AND active AND NOT startsWith(column, '_') ORDER BY column;

DROP TABLE t_phys_dedup;

-- Test 10: Default expression referencing another column + rename
SELECT 'Test 10: default expression with rename';
DROP TABLE IF EXISTS t_phys_defaults;

CREATE TABLE t_phys_defaults
(
    a UInt64,
    b UInt64,
    c UInt64 DEFAULT b * 2
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_phys_defaults (a, b) VALUES (1, 10);
SELECT a, b, c FROM t_phys_defaults ORDER BY a;

ALTER TABLE t_phys_defaults RENAME COLUMN b TO val;

SELECT column, physical_name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_phys_defaults' AND active AND NOT startsWith(column, '_') ORDER BY column;

INSERT INTO t_phys_defaults (a, val) VALUES (2, 20);
SELECT a, val, c FROM t_phys_defaults ORDER BY a;

DROP TABLE t_phys_defaults;

-- Test 11: Flattened Nested ADD works with compound physical names.
SELECT 'Test 11: flattened Nested with compound names';
DROP TABLE IF EXISTS t_phys_flat_nested;
CREATE TABLE t_phys_flat_nested (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
INSERT INTO t_phys_flat_nested VALUES (1);
ALTER TABLE t_phys_flat_nested ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_phys_flat_nested VALUES (2, [10, 20], ['a', 'b']);
SELECT a, `n.x`, `n.y` FROM t_phys_flat_nested ORDER BY a;
DROP TABLE t_phys_flat_nested;

-- Test 12: DROP + re-ADD same column in a single ALTER falls back to mutation
-- (reusing the same logical name means the physical name stays identity-mapped,
-- but the mutation rewrites data so the old values are replaced by the default).
SELECT 'Test 12: drop and re-add in single ALTER';
DROP TABLE IF EXISTS t_phys_drop_readd;
CREATE TABLE t_phys_drop_readd (a UInt64, b String) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
INSERT INTO t_phys_drop_readd VALUES (1, 'old_data');
ALTER TABLE t_phys_drop_readd DROP COLUMN b, ADD COLUMN b String DEFAULT 'new_default';
INSERT INTO t_phys_drop_readd VALUES (2, 'inserted');
SELECT a, b FROM t_phys_drop_readd ORDER BY a;
SELECT column, physical_name FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_phys_drop_readd' AND active AND column = 'b' AND NOT startsWith(column, '_')
    ORDER BY name;
OPTIMIZE TABLE t_phys_drop_readd FINAL;
SELECT a, b FROM t_phys_drop_readd ORDER BY a;
DROP TABLE t_phys_drop_readd;

-- Test 13: DROP + re-ADD Nested column in single ALTER forces mutation
-- Regression: previously, DROP COLUMN n (parent) was not detected in
-- new_col_names which contained expanded n.x, n.y — bypassing force_mutation.
SELECT 'Test 13: drop and re-add Nested in single ALTER';
DROP TABLE IF EXISTS t_phys_drop_readd_nested;
CREATE TABLE t_phys_drop_readd_nested (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
INSERT INTO t_phys_drop_readd_nested VALUES (1, [10, 20], ['a', 'b']);
ALTER TABLE t_phys_drop_readd_nested DROP COLUMN n, ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_phys_drop_readd_nested VALUES (2, [30, 40], ['c', 'd']);
SELECT a, `n.x`, `n.y` FROM t_phys_drop_readd_nested ORDER BY a;
OPTIMIZE TABLE t_phys_drop_readd_nested FINAL;
SELECT a, `n.x`, `n.y` FROM t_phys_drop_readd_nested ORDER BY a;
DROP TABLE t_phys_drop_readd_nested;

-- Test 14: compact Variant column with escape_variant_subcolumn_filenames change across rename
-- Regression: ColumnsSubstreams.cpp missed the physical-name retry after toggling
-- escape_variant_substreams, making valid compact parts unreadable.
SELECT 'Test 14: compact Variant with escape setting change across rename';
DROP TABLE IF EXISTS t_phys_variant_compact;
CREATE TABLE t_phys_variant_compact
(
    a UInt64,
    v Variant(String, UInt64)
)
ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1,
    escape_variant_subcolumn_filenames = 0;

INSERT INTO t_phys_variant_compact VALUES (1, 'hello'), (2, 42);
SELECT a, v, variantType(v) FROM t_phys_variant_compact ORDER BY a;

ALTER TABLE t_phys_variant_compact MODIFY SETTING escape_variant_subcolumn_filenames = 1;
ALTER TABLE t_phys_variant_compact RENAME COLUMN v TO w;

INSERT INTO t_phys_variant_compact VALUES (3, 'world'), (4, 99);
SELECT a, w, variantType(w) FROM t_phys_variant_compact ORDER BY a;

OPTIMIZE TABLE t_phys_variant_compact FINAL;
SELECT a, w, variantType(w) FROM t_phys_variant_compact ORDER BY a;

DROP TABLE t_phys_variant_compact;

-- Test 15: Full DETACH TABLE / ATTACH TABLE — verifies physical_names.json
-- survives a full table detach-attach cycle (mapping loaded from disk).
SELECT 'Test 15: full table detach/attach recovery';
DROP TABLE IF EXISTS t_phys_full_detach;
CREATE TABLE t_phys_full_detach (a UInt64, b String) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
INSERT INTO t_phys_full_detach VALUES (1, 'before');
ALTER TABLE t_phys_full_detach RENAME COLUMN b TO d;
INSERT INTO t_phys_full_detach (a, d) VALUES (2, 'after');
SELECT a, d FROM t_phys_full_detach ORDER BY a;
DETACH TABLE t_phys_full_detach;
ATTACH TABLE t_phys_full_detach;
SELECT a, d FROM t_phys_full_detach ORDER BY a;
OPTIMIZE TABLE t_phys_full_detach FINAL;
SELECT a, d FROM t_phys_full_detach ORDER BY a;
DROP TABLE t_phys_full_detach;

-- Test 16: TTL column rename — verify TTL still works after renaming a
-- non-TTL column (the TTL expression references a different column).
-- Use INTERVAL 50 YEAR to stay within DateTime 32-bit range (max ~2106).
SELECT 'Test 16: TTL with column rename';
DROP TABLE IF EXISTS t_phys_ttl;
CREATE TABLE t_phys_ttl
(
    a UInt64,
    b String,
    dt DateTime DEFAULT now()
)
ENGINE = MergeTree ORDER BY a
TTL dt + INTERVAL 50 YEAR
SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
INSERT INTO t_phys_ttl (a, b) VALUES (1, 'hello');
ALTER TABLE t_phys_ttl RENAME COLUMN b TO d;
INSERT INTO t_phys_ttl (a, d) VALUES (2, 'world');
SELECT a, d FROM t_phys_ttl ORDER BY a;
OPTIMIZE TABLE t_phys_ttl FINAL;
SELECT a, d FROM t_phys_ttl ORDER BY a;
DROP TABLE t_phys_ttl;

-- Test 17: Single-child flattened Nested rename across parents is rejected.
-- When a single-child Nested column is ADDed after physical names activation,
-- it gets a plain counter physical name (no dot). Renaming it across parent
-- boundaries would break offset stream lookup, so the system rejects it.
SELECT 'Test 17: single-child Nested rename rejection';
DROP TABLE IF EXISTS t_phys_nested_single;
CREATE TABLE t_phys_nested_single
(
    a UInt64
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
ALTER TABLE t_phys_nested_single ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_phys_nested_single VALUES (1, [10, 20, 30]);
ALTER TABLE t_phys_nested_single RENAME COLUMN `n.x` TO `m.x`; -- { serverError NOT_IMPLEMENTED }
SELECT a, `n.x` FROM t_phys_nested_single;
DROP TABLE t_phys_nested_single;

-- Test 18: Multi-child flattened Nested rename across parents works fine
-- because compound physical names (e.g. "5.x", "5.y") keep the offset
-- stream stable.
SELECT 'Test 18: multi-child Nested rename across parents';
DROP TABLE IF EXISTS t_phys_nested_multi;
CREATE TABLE t_phys_nested_multi
(
    a UInt64,
    `n.x` Array(UInt64),
    `n.y` Array(String)
)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;
INSERT INTO t_phys_nested_multi VALUES (1, [10, 20], ['aa', 'bb']);
ALTER TABLE t_phys_nested_multi RENAME COLUMN `n.x` TO `m.x`;
ALTER TABLE t_phys_nested_multi RENAME COLUMN `n.y` TO `m.y`;
INSERT INTO t_phys_nested_multi VALUES (2, [30], ['cc']);
SELECT a, `m.x`, `m.y` FROM t_phys_nested_multi ORDER BY a;
OPTIMIZE TABLE t_phys_nested_multi FINAL;
SELECT a, `m.x`, `m.y` FROM t_phys_nested_multi ORDER BY a;
DROP TABLE t_phys_nested_multi;
