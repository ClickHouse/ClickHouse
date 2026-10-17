-- The column names in exclude_materialize_statistics_on_insert and exclude_materialize_statistics_on_merge:
-- the whole value must be a comma-separated list of names, a name may be dotted (a column of a Nested
-- structure), quoted or a string literal, names are case-sensitive and unknown names are ignored.
SET allow_statistics = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    a UInt64,
    n Nested(x UInt64, y UInt64),
    `d.z` UInt64,
    `c,ol` UInt64,
    B UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;

-- Not a list of column names. These were silently read as a prefix of the list before ('a b' as 'a').
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = 'a b'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = 'a,'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = ',a'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = 'a;b'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = '1+1'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = 'n.'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = 'n.1'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = '\'\''; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = '`a'; -- { serverError CANNOT_PARSE_TEXT }
SELECT 'Rows inserted with an invalid list', count() FROM tab;

-- An empty list, also with only whitespace or a comment, excludes nothing.
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = '  ';
INSERT INTO tab SELECT 1, [1], [1], 1, 1, 1 SETTINGS exclude_materialize_statistics_on_insert = '/* nothing */';
SELECT 'Columns without statistics after inserts with an empty list', countIf(statistics = []) FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active;
TRUNCATE TABLE tab;

SELECT 'INSERT excludes n.x, d.z, c,ol, but not B, because the list names b';
INSERT INTO tab SELECT number, [number], [number], number, number, number FROM numbers(10)
SETTINGS exclude_materialize_statistics_on_insert = 'n.x, `d.z`, \'c,ol\', no_such_column, b';
SELECT DISTINCT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

TRUNCATE TABLE tab;

SET materialize_statistics_on_insert = 0;

ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'a b'; -- { serverError CANNOT_PARSE_TEXT }
ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'n.1'; -- { serverError CANNOT_PARSE_TEXT }

ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = '`n`.`y`, d.z, A';

SYSTEM STOP MERGES tab;
INSERT INTO tab SELECT number, [number], [number], number, number, number FROM numbers(10);
INSERT INTO tab SELECT number, [number], [number], number, number, number FROM numbers(10, 10);
SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT 'Merge excludes n.y and d.z, but not a, because the list names A';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

DROP TABLE tab;
