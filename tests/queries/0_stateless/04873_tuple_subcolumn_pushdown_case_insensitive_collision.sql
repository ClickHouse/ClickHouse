-- Tags: no-fasttest

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
-- The pass disables itself for the whole query when a join can wrap results in Nullable, so
-- stress workers setting join_use_nulls = 1 would make the positive-control arm assert 0.
SET join_use_nulls = 0;

-- A top-level column whose name matches the flattened tuple-element name up to case must block
-- the rewrite: a reader with case-insensitive column matching (e.g.
-- input_format_orc_case_insensitive_column_matching) binds `a.b` to that column instead of the
-- tuple element, turning the optimization into wrong values and wrong pruning.

SELECT '-- collision with a top-level column differing only by case blocks the rewrite';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'b') FROM file('nonexistent_04873.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)'))
WHERE explain ILIKE '%column_name: a.b%';

SELECT '-- exact-case collision stays blocked';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'b') FROM file('nonexistent_04873.orc', ORC, '`a.b` UInt64, a Tuple(b UInt64)'))
WHERE explain ILIKE '%column_name: a.b%';

SELECT '-- no collision: the rewrite fires';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT tupleElement(a, 'b') FROM file('nonexistent_04873.orc', ORC, '`c.d` UInt64, a Tuple(b UInt64)'))
WHERE explain ILIKE '%column_name: a.b%';

SELECT '-- ORC: case-insensitive matching returns the tuple element, not the colliding column';
INSERT INTO FUNCTION file('04873_collision.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)')
SELECT number + 100, tuple(number + 200) FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT tupleElement(a, 'b') FROM file('04873_collision.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)')
ORDER BY 1
SETTINGS input_format_orc_case_insensitive_column_matching = 1;

SELECT count() FROM file('04873_collision.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)')
WHERE tupleElement(a, 'b') = 201
SETTINGS input_format_orc_case_insensitive_column_matching = 1;

SELECT '-- Parquet: same';
INSERT INTO FUNCTION file('04873_collision.parquet', Parquet, '`A.B` UInt64, a Tuple(b UInt64)')
SELECT number + 100, tuple(number + 200) FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT tupleElement(a, 'b') FROM file('04873_collision.parquet', Parquet, '`A.B` UInt64, a Tuple(b UInt64)')
ORDER BY 1
SETTINGS input_format_parquet_case_insensitive_column_matching = 1;

SELECT count() FROM file('04873_collision.parquet', Parquet, '`A.B` UInt64, a Tuple(b UInt64)')
WHERE tupleElement(a, 'b') = 201
SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
