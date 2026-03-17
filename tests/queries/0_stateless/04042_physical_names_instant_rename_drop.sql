-- Test instant RENAME: no mutation produced
DROP TABLE IF EXISTS t_instant_rename;

CREATE TABLE t_instant_rename
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

INSERT INTO t_instant_rename VALUES (1, 'one');
INSERT INTO t_instant_rename VALUES (2, 'two');

ALTER TABLE t_instant_rename RENAME COLUMN b TO d;

SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_instant_rename' AND NOT is_done;
SELECT a, d FROM t_instant_rename ORDER BY a;

-- Test RENAME then INSERT then SELECT across parts
INSERT INTO t_instant_rename VALUES (3, 'three');
SELECT a, d FROM t_instant_rename ORDER BY a;

-- Test chained RENAME preserves identity
ALTER TABLE t_instant_rename RENAME COLUMN d TO e;
SELECT a, e FROM t_instant_rename ORDER BY a;

-- Merge after rename
OPTIMIZE TABLE t_instant_rename FINAL;
SELECT a, e FROM t_instant_rename ORDER BY a;

DROP TABLE t_instant_rename;

-- Test instant DROP: no mutation
DROP TABLE IF EXISTS t_instant_drop;

CREATE TABLE t_instant_drop
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

INSERT INTO t_instant_drop VALUES (1, 'one', 10);
ALTER TABLE t_instant_drop DROP COLUMN c;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_instant_drop' AND NOT is_done;
SELECT * FROM t_instant_drop ORDER BY a;

DROP TABLE t_instant_drop;

-- Test DROP + re-ADD same name: old data invisible
DROP TABLE IF EXISTS t_drop_readd;

CREATE TABLE t_drop_readd
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

INSERT INTO t_drop_readd VALUES (1, 'old_value');
ALTER TABLE t_drop_readd DROP COLUMN b;
ALTER TABLE t_drop_readd ADD COLUMN b String DEFAULT 'new_default';
INSERT INTO t_drop_readd VALUES (2, 'new_value');
SELECT a, b FROM t_drop_readd ORDER BY a;

DROP TABLE t_drop_readd;

-- Test lazy activation for existing tables
DROP TABLE IF EXISTS t_lazy_activate;

CREATE TABLE t_lazy_activate
(
    a UInt64,
    b String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0;

INSERT INTO t_lazy_activate VALUES (1, 'one');

ALTER TABLE t_lazy_activate MODIFY SETTING
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

-- Activation happens on first compatible ALTER
ALTER TABLE t_lazy_activate ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_lazy_activate (a, b, c) VALUES (2, 'two', 22);

-- Now RENAME should be instant (no mutation)
ALTER TABLE t_lazy_activate RENAME COLUMN b TO d;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_lazy_activate' AND NOT is_done;
SELECT a, d, c FROM t_lazy_activate ORDER BY a;

DROP TABLE t_lazy_activate;

-- Test MODIFY COLUMN still creates mutation (not affected by physical names bypass)
DROP TABLE IF EXISTS t_modify_still_mutates;

CREATE TABLE t_modify_still_mutates
(
    a UInt64,
    b UInt32
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_physical_names',
    activate_physical_names_for_existing_tables = 1;

INSERT INTO t_modify_still_mutates VALUES (1, 10);
ALTER TABLE t_modify_still_mutates MODIFY COLUMN b UInt64;
SELECT b, toTypeName(b) FROM t_modify_still_mutates;

DROP TABLE t_modify_still_mutates;
