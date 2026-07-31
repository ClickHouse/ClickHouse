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

DROP TABLE tab;
