-- Tags: zookeeper
-- This is a whole separate new set of tests since ReplicatedMergeTree deals with the metadata changes differently (via Keeper)

-- Group 1: Column operations (rename, add, drop, multiple ops) - reuse one table
DROP TABLE IF EXISTS t_column_ops SYNC;

CREATE TABLE t_column_ops (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_column_ops', '1')
ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_column_ops VALUES (1, 2), (3, 4);

-- Test 1: Rename column with implicit indices
SELECT 'Initial indices:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_column_ops'
ORDER BY name;

ALTER TABLE t_column_ops RENAME COLUMN b TO c;
SYSTEM SYNC REPLICA t_column_ops;

SELECT 'After rename:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_column_ops'
ORDER BY name;

SELECT * FROM t_column_ops ORDER BY a;

-- Test 2: Add column with implicit indices
ALTER TABLE t_column_ops ADD COLUMN b UInt64 DEFAULT 0;
SYSTEM SYNC REPLICA t_column_ops;

SELECT 'After adding column:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_column_ops'
ORDER BY name;

SELECT * FROM t_column_ops ORDER BY a;

-- Test 5: Drop column with implicit index
ALTER TABLE t_column_ops DROP COLUMN b;
SYSTEM SYNC REPLICA t_column_ops;

SELECT 'After dropping column b:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_column_ops'
ORDER BY name;

SELECT * FROM t_column_ops ORDER BY a;

-- Test 6: Multiple operations at once (rename + add column)
-- Rename c to old_b and add new column d
ALTER TABLE t_column_ops RENAME COLUMN c TO old_b, ADD COLUMN d UInt64 DEFAULT 0;
SYSTEM SYNC REPLICA t_column_ops;

SELECT 'After rename and add:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_column_ops'
ORDER BY name;

SELECT * FROM t_column_ops ORDER BY a;

DROP TABLE t_column_ops SYNC;

-- Group 2: Type modification test
DROP TABLE IF EXISTS t_type_ops SYNC;

CREATE TABLE t_type_ops (a UInt64, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_type_ops', '1')
ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1, add_minmax_index_for_string_columns=0;

INSERT INTO t_type_ops VALUES (1, 2), (3, 4);

-- Test 3: Modify column type from numeric to non-numeric (implicit index should be removed)
SELECT 'Before type change:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_type_ops'
ORDER BY name;

ALTER TABLE t_type_ops MODIFY COLUMN b String;
SYSTEM SYNC REPLICA t_type_ops;

SELECT 'After type change to String:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_type_ops'
ORDER BY name;

SELECT * FROM t_type_ops ORDER BY a;

DROP TABLE t_type_ops SYNC;

-- Group 3: Explicit index interactions - reuse one table
DROP TABLE IF EXISTS t_explicit_ops SYNC;

CREATE TABLE t_explicit_ops (a UInt64, b UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_explicit_ops', '1')
ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_explicit_ops VALUES (1, 2), (3, 4);

-- Test 7: Explicit index on same column (should not create duplicate implicit index)
SELECT 'Before adding explicit index:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_explicit_ops'
ORDER BY name;

-- Add explicit minmax index on column b (should not duplicate the implicit one)
ALTER TABLE t_explicit_ops ADD INDEX idx_explicit_b b TYPE minmax GRANULARITY 1;
SYSTEM SYNC REPLICA t_explicit_ops;

SELECT 'After adding explicit index on b:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_explicit_ops'
ORDER BY name;

-- Verify only one index exists per column
SELECT column_name, count() as index_count FROM (
    SELECT arrayJoin(splitByChar(',', expr)) as column_name
    FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_explicit_ops'
    AND type = 'minmax'
)
GROUP BY column_name
ORDER BY column_name;

-- Test 8: Drop explicit index (implicit should remain/regenerate)
ALTER TABLE t_explicit_ops DROP INDEX idx_explicit_b;
SYSTEM SYNC REPLICA t_explicit_ops;

SELECT 'After dropping explicit index:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_explicit_ops'
ORDER BY name;

-- Test 4: Add both explicit index and new column simultaneously
-- Add both a new column and a new explicit index
ALTER TABLE t_explicit_ops ADD COLUMN c UInt64 DEFAULT 0, ADD INDEX idx_explicit_a a TYPE minmax GRANULARITY 1;
SYSTEM SYNC REPLICA t_explicit_ops;

SELECT 'After adding index and column:';
SELECT name, type, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_explicit_ops'
ORDER BY name;

SELECT * FROM t_explicit_ops ORDER BY a;

DROP TABLE t_explicit_ops SYNC;
