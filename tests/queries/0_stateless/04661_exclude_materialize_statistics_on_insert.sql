-- Analogous to 02346_exclude_materialize_skip_indexes_on_insert, but for column statistics.
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    a UInt64,
    b UInt64,
    `c,ol` String
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'basic';

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100)
SETTINGS exclude_materialize_statistics_on_insert = '!@#$^#$&#$$%$,,.,3.45,45.'; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100)
SETTINGS exclude_materialize_statistics_on_insert = 'b c'; -- { serverError CANNOT_PARSE_TEXT }
-- The list is checked also when the table is too large to build statistics on INSERT.
INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100)
SETTINGS exclude_materialize_statistics_on_insert = 'b c', materialize_statistics_on_insert_max_table_size = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT 'Rows inserted with an invalid list', count() FROM tab;

-- A table without statistics does not use the list, so an invalid one does not affect INSERTs into it.
DROP TABLE IF EXISTS tab_no_stats;
CREATE TABLE tab_no_stats (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
INSERT INTO tab_no_stats SELECT number, number FROM numbers(10) SETTINGS exclude_materialize_statistics_on_insert = 'b c';
SELECT 'Rows inserted into a table without statistics', count() FROM tab_no_stats;
DROP TABLE tab_no_stats;

SET exclude_materialize_statistics_on_insert = 'b';

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100);

SELECT 'Column b is excluded on INSERT, so only a and `c,ol` have statistics';
SELECT column, min(statistics != []) AS has_stats_on_all_parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
GROUP BY column
ORDER BY column;

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT 'After OPTIMIZE FINAL, merge materializes statistics for all columns including b';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

TRUNCATE TABLE tab;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100);

-- Mutations require merges to be allowed; start them before MATERIALIZE STATISTICS.
SYSTEM START MERGES tab;
ALTER TABLE tab MATERIALIZE STATISTICS b;

SELECT 'MATERIALIZE STATISTICS builds excluded columns despite the insert exclude setting';
SELECT column, min(statistics != []) AS has_stats_on_all_parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
GROUP BY column
ORDER BY column;

TRUNCATE TABLE tab;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100)
SETTINGS exclude_materialize_statistics_on_insert = '`c,ol`';
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100)
SETTINGS exclude_materialize_statistics_on_insert = '`c,ol`';

SELECT 'Query-level setting overrides session setting: `c,ol` excluded, b included';
SELECT column, min(statistics != []) AS has_stats_on_all_parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
GROUP BY column
ORDER BY column;

TRUNCATE TABLE tab;

SET exclude_materialize_statistics_on_insert = 'b, `c,ol`';

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100);

SELECT 'Both b and `c,ol` are excluded on INSERT';
SELECT column, min(statistics != []) AS has_stats_on_all_parts
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
GROUP BY column
ORDER BY column;

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT 'After OPTIMIZE FINAL, merge materializes statistics for all columns';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

TRUNCATE TABLE tab;
SET exclude_materialize_statistics_on_insert = DEFAULT;

-- The setting is not tied to a table: it applies to every table the INSERT writes to, here also to
-- the target table of a materialized view.
DROP TABLE IF EXISTS dst;
DROP VIEW IF EXISTS mv;
CREATE TABLE dst (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0, auto_statistics_types = 'basic';
CREATE MATERIALIZED VIEW mv TO dst AS SELECT a, b FROM tab;

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100)
SETTINGS exclude_materialize_statistics_on_insert = 'b';

SELECT 'The materialized view target is affected as well';
SELECT DISTINCT table, column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('tab', 'dst') AND active
ORDER BY table, column;

DROP VIEW mv;
DROP TABLE dst;
DROP TABLE tab;
