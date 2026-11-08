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
INSERT INTO FUNCTION file(currentDatabase() || '_04873_collision.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)')
SELECT number + 100, tuple(number + 200) FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT tupleElement(a, 'b') FROM file(currentDatabase() || '_04873_collision.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)')
ORDER BY 1
SETTINGS input_format_orc_case_insensitive_column_matching = 1;

SELECT count() FROM file(currentDatabase() || '_04873_collision.orc', ORC, '`A.B` UInt64, a Tuple(b UInt64)')
WHERE tupleElement(a, 'b') = 201
SETTINGS input_format_orc_case_insensitive_column_matching = 1;

SELECT '-- Parquet: same';
INSERT INTO FUNCTION file(currentDatabase() || '_04873_collision.parquet', Parquet, '`A.B` UInt64, a Tuple(b UInt64)')
SELECT number + 100, tuple(number + 200) FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT tupleElement(a, 'b') FROM file(currentDatabase() || '_04873_collision.parquet', Parquet, '`A.B` UInt64, a Tuple(b UInt64)')
ORDER BY 1
SETTINGS input_format_parquet_case_insensitive_column_matching = 1;

SELECT count() FROM file(currentDatabase() || '_04873_collision.parquet', Parquet, '`A.B` UInt64, a Tuple(b UInt64)')
WHERE tupleElement(a, 'b') = 201
SETTINGS input_format_parquet_case_insensitive_column_matching = 1;

-- The guards above read the declared structure, which is not the physical schema: a structure that
-- omits a colliding top-level column hides the collision from the analyzer, and then only the reader
-- sees the ambiguous flattened name. Pin what each reader does with it, so that neither starts
-- silently returning the colliding top-level column instead of the tuple element.
-- The files below hold 90x in the flat `a.b` and 10x in the tuple element that flattens to `a.b`.
SELECT '-- a structure that omits the colliding column: ORC returns the tuple element';
INSERT INTO FUNCTION file(currentDatabase() || '_04873_hidden.orc', ORC, '`a.b` UInt64, a Tuple(b UInt64)')
SELECT number + 900, tuple(number + 100) FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT tupleElement(a, 'b') FROM file(currentDatabase() || '_04873_hidden.orc', ORC, 'a Tuple(b UInt64)')
ORDER BY 1;

SELECT count() FROM file(currentDatabase() || '_04873_hidden.orc', ORC, 'a Tuple(b UInt64)')
WHERE tupleElement(a, 'b') = 101;

SELECT count() FROM file(currentDatabase() || '_04873_hidden.orc', ORC, 'a Tuple(b UInt64)')
WHERE tupleElement(a, 'b') = 901;

SELECT '-- a structure that omits the colliding column: Parquet refuses the ambiguous name';
INSERT INTO FUNCTION file(currentDatabase() || '_04873_hidden.parquet', Parquet, '`a.b` UInt64, a Tuple(b UInt64)')
SELECT number + 900, tuple(number + 100) FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT tupleElement(a, 'b') FROM file(currentDatabase() || '_04873_hidden.parquet', Parquet, 'a Tuple(b UInt64)')
ORDER BY 1; -- { serverError DUPLICATE_COLUMN }

-- Declaring the colliding column brings the collision back into view, and the guard blocks the
-- rewrite, so the same read succeeds and returns the tuple element.
SELECT tupleElement(a, 'b') FROM file(currentDatabase() || '_04873_hidden.parquet', Parquet, '`a.b` UInt64, a Tuple(b UInt64)')
ORDER BY 1;
