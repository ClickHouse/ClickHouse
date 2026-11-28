-- Tests that system.tables.skipping_indices_types reports the distinct, sorted
-- set of data skipping index types defined on a table.

DROP TABLE IF EXISTS t_skip_idx_types;
DROP TABLE IF EXISTS t_no_skip_idx;
DROP TABLE IF EXISTS t_not_mergetree;

-- Several skip indices, including two of the same type (minmax) to check deduplication.
CREATE TABLE t_skip_idx_types
(
    a UInt64,
    b String,
    c UInt64,
    INDEX idx_mm1 a TYPE minmax GRANULARITY 1,
    INDEX idx_set b TYPE set(100) GRANULARITY 1,
    INDEX idx_bf b TYPE bloom_filter GRANULARITY 1,
    INDEX idx_mm2 c TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a;

-- A MergeTree table without any skip indices.
CREATE TABLE t_no_skip_idx (a UInt64) ENGINE = MergeTree ORDER BY a;

-- A non-MergeTree table cannot have skip indices.
CREATE TABLE t_not_mergetree (a UInt64) ENGINE = Memory;

SELECT name, skipping_indices_types
FROM system.tables
WHERE database = currentDatabase() AND name IN ('t_skip_idx_types', 't_no_skip_idx', 't_not_mergetree')
ORDER BY name;

DROP TABLE t_skip_idx_types;
DROP TABLE t_no_skip_idx;
DROP TABLE t_not_mergetree;

-- Session temporary MergeTree table with skip indices must report the types too.
DROP TEMPORARY TABLE IF EXISTS t_tmp_skip_idx;
CREATE TEMPORARY TABLE t_tmp_skip_idx
(
    a UInt64,
    b String,
    INDEX idx_mm a TYPE minmax GRANULARITY 1,
    INDEX idx_set b TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a;

SELECT name, skipping_indices_types
FROM system.tables
WHERE is_temporary AND name = 't_tmp_skip_idx';

DROP TEMPORARY TABLE t_tmp_skip_idx;

-- Implicitly created skip indices (via add_minmax_index_for_numeric_columns) must NOT be
-- reported: the column lists only skip indices explicitly defined on the table.
DROP TABLE IF EXISTS t_implicit_minmax;
DROP TABLE IF EXISTS t_implicit_and_explicit;

CREATE TABLE t_implicit_minmax (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1;

-- Same auto-minmax setting, but also one explicitly defined set index: only 'set' is reported.
CREATE TABLE t_implicit_and_explicit
(
    a UInt64,
    b String,
    INDEX idx_set b TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1;

SELECT name, skipping_indices_types
FROM system.tables
WHERE database = currentDatabase() AND name IN ('t_implicit_minmax', 't_implicit_and_explicit')
ORDER BY name;

DROP TABLE t_implicit_minmax;
DROP TABLE t_implicit_and_explicit;
