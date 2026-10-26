-- exclude_materialize_statistics_on_insert and exclude_materialize_statistics_on_merge exclude statistics declared
-- explicitly with STATISTICS(...) as well, not only those added by auto_statistics_types.
SET allow_statistics = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    a UInt64,
    b UInt64 STATISTICS(tdigest),
    c UInt64 STATISTICS(tdigest)
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = '',
    exclude_materialize_statistics_on_merge = 'c';

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, number FROM numbers(100)
SETTINGS materialize_statistics_on_insert = 1, exclude_materialize_statistics_on_insert = 'b';

SELECT 'INSERT excludes b';
SELECT DISTINCT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

INSERT INTO tab SELECT number + 100, number + 100, number + 100 FROM numbers(100)
SETTINGS materialize_statistics_on_insert = 0;

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT 'Merge excludes c';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab' AND active
ORDER BY column;

DROP TABLE tab;
