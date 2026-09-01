-- Tags: no-random-merge-tree-settings
-- map[key] must return the value of the FIRST occurrence of the key in the row, and the
-- result must not depend on optimize_functions_to_subcolumns, on preceding rows in the
-- block, or on whether a filter isolates the row (issue #111203).

DROP TABLE IF EXISTS t_map_dup;

-- Wide part (min_bytes_for_wide_part = 0). Row 1 primes the cross-row key-position state:
-- its match for '' sits at relative position 1. Row 2 has '' at positions 0 and 1, so a
-- prediction of position 1 would wrongly return the second occurrence.
CREATE TABLE t_map_dup (id UInt64, m Map(String, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_dup VALUES (1, map('x', 'k', '', 'prime')), (2, map('', 'FIRST', '', 'SECOND'));

SELECT 'wide, filter id=2, subcolumns=0', m[''] FROM t_map_dup WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'wide, filter id=2, subcolumns=1', m[''] FROM t_map_dup WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'wide, no filter, subcolumns=0', id, m[''] FROM t_map_dup ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'wide, no filter, subcolumns=1', id, m[''] FROM t_map_dup ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
-- The issue's count() example: filtering on the first duplicate's value must match exactly one row.
SELECT 'wide, count first value, subcolumns=0', count() FROM t_map_dup WHERE id = 2 AND m[''] = 'FIRST' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'wide, count first value, subcolumns=1', count() FROM t_map_dup WHERE id = 2 AND m[''] = 'FIRST' SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_dup;

-- Compact part (default min_bytes_for_wide_part) exercises the other reader path.
CREATE TABLE t_map_dup (id UInt64, m Map(String, String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_map_dup VALUES (1, map('x', 'k', '', 'prime')), (2, map('', 'FIRST', '', 'SECOND'));

SELECT 'compact, filter id=2, subcolumns=0', m[''] FROM t_map_dup WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'compact, filter id=2, subcolumns=1', m[''] FROM t_map_dup WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'compact, no filter, subcolumns=0', id, m[''] FROM t_map_dup ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'compact, no filter, subcolumns=1', id, m[''] FROM t_map_dup ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_dup;

-- Value-type matrix: the first-match value must be returned for every wrapper. Each table
-- primes the key position at 1 in row 1, then has a duplicate key in row 2.
DROP TABLE IF EXISTS t_map_types;

-- Nullable(String) value
CREATE TABLE t_map_types (id UInt64, m Map(String, Nullable(String)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map('x', 'k', 'd', 'prime')), (2, map('d', 'FIRST', 'd', 'SECOND'));
SELECT 'Nullable(String) subcolumns=0', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Nullable(String) subcolumns=1', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;

-- LowCardinality(String) value
CREATE TABLE t_map_types (id UInt64, m Map(String, LowCardinality(String)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map('x', 'k', 'd', 'prime')), (2, map('d', 'FIRST', 'd', 'SECOND'));
SELECT 'LowCardinality(String) subcolumns=0', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'LowCardinality(String) subcolumns=1', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;

-- LowCardinality(Nullable(String)) value
CREATE TABLE t_map_types (id UInt64, m Map(String, LowCardinality(Nullable(String))))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map('x', 'k', 'd', 'prime')), (2, map('d', 'FIRST', 'd', 'SECOND'));
SELECT 'LowCardinality(Nullable(String)) subcolumns=0', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'LowCardinality(Nullable(String)) subcolumns=1', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;

-- UInt64 value
CREATE TABLE t_map_types (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map('x', 10, 'd', 20)), (2, map('d', 111, 'd', 222));
SELECT 'UInt64 subcolumns=0', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'UInt64 subcolumns=1', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;

-- Array(String) value
CREATE TABLE t_map_types (id UInt64, m Map(String, Array(String)))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map('x', ['k'], 'd', ['prime'])), (2, map('d', ['FIRST'], 'd', ['SECOND']));
SELECT 'Array(String) subcolumns=0', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'Array(String) subcolumns=1', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;

-- Key-type matrix.

-- LowCardinality(String) key
CREATE TABLE t_map_types (id UInt64, m Map(LowCardinality(String), String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map('x', 'k', 'd', 'prime')), (2, map('d', 'FIRST', 'd', 'SECOND'));
SELECT 'LowCardinality(String) key subcolumns=0', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'LowCardinality(String) key subcolumns=1', m['d'] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;

-- UInt64 key
CREATE TABLE t_map_types (id UInt64, m Map(UInt64, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map_types VALUES (1, map(9, 'k', 7, 'prime')), (2, map(7, 'FIRST', 7, 'SECOND'));
SELECT 'UInt64 key subcolumns=0', m[7] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'UInt64 key subcolumns=1', m[7] FROM t_map_types WHERE id = 2 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_map_types;
