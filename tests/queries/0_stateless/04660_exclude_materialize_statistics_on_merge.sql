-- Analogous to 02346_exclude_materialize_skip_indexes_on_merge, but for column statistics.
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 0;
SET mutations_sync = 2;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_invalid;

-- An invalid list is rejected when the table is created or altered, so that merges never fail because of it.
CREATE TABLE tab_invalid (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS exclude_materialize_statistics_on_merge = 'a b'; -- { serverError CANNOT_PARSE_TEXT }

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
    auto_statistics_types = 'basic',
    materialize_statistics_on_merge = 1;

ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = '!@#$^#$&#$$%$,,.,3.45,45.'; -- { serverError CANNOT_PARSE_TEXT }
ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'b c'; -- { serverError CANNOT_PARSE_TEXT }
ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'b,'; -- { serverError CANNOT_PARSE_TEXT }
ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = '1+1'; -- { serverError CANNOT_PARSE_TEXT }
SELECT 'The table setting is unchanged after the rejected ALTERs', engine_full NOT LIKE '%exclude_materialize_statistics_on_merge%'
FROM system.tables WHERE database = currentDatabase() AND name = 'tab';

ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'b';

-- A background merge would build statistics before the first check.
SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100);

SELECT 'After INSERT with materialize_statistics_on_insert=0, no part has statistics yet';
SELECT column, max(statistics != []) AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
GROUP BY column
ORDER BY column;

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT 'After OPTIMIZE FINAL, column b is excluded from merge materialization';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

ALTER TABLE tab MATERIALIZE STATISTICS b;

SELECT 'After explicit MATERIALIZE STATISTICS b, all columns have statistics';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

-- The setting does not affect INSERTs, so the new part gets statistics for b too.
INSERT INTO tab SELECT number + 200, number + 200, toString(number + 200) FROM numbers(100)
SETTINGS materialize_statistics_on_insert = 1;
OPTIMIZE TABLE tab FINAL;

SELECT 'A merge drops the statistics of b, although all merged parts had them';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

TRUNCATE TABLE tab;

ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'b, `c,ol`';

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100);
OPTIMIZE TABLE tab FINAL;

SELECT 'Both b and `c,ol` are excluded from merge materialization';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

-- Mutations do not use the setting: rewriting b builds its statistics.
ALTER TABLE tab UPDATE b = b + 1 WHERE 1;

SELECT 'A mutation that rewrites b builds statistics for it';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active AND column = 'b';

DROP TABLE tab;
