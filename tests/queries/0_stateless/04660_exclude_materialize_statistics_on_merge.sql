-- Analogous to 02346_exclude_materialize_skip_indexes_on_merge, but for column statistics.
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 0;
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
    auto_statistics_types = 'basic',
    materialize_statistics_on_merge = 1;

-- negative test case
ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = '!@#$^#$&#$$%$,,.,3.45,45.';
INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
OPTIMIZE TABLE tab FINAL; -- { serverError CANNOT_PARSE_TEXT }
TRUNCATE TABLE tab;

ALTER TABLE tab MODIFY SETTING exclude_materialize_statistics_on_merge = 'b';

INSERT INTO tab SELECT number, number, toString(number) FROM numbers(100);
INSERT INTO tab SELECT number + 100, number + 100, toString(number + 100) FROM numbers(100);

SELECT 'After INSERT with materialize_statistics_on_insert=0, no part has statistics yet';
SELECT column, max(statistics != []) AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
GROUP BY column
ORDER BY column;

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

DROP TABLE tab;
