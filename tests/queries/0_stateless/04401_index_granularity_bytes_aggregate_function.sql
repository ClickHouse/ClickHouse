-- Tags: no-random-merge-tree-settings, long
-- index_granularity_bytes must be honored for AggregateFunction state columns, whose true size
-- ColumnAggregateFunction::byteSize() cannot report (states live in shared arenas). See #108243, #20013.

DROP TABLE IF EXISTS amt_granule;

CREATE TABLE amt_granule (g UInt64, s AggregateFunction(uniqExact, UInt64))
ENGINE = AggregatingMergeTree ORDER BY g
SETTINGS index_granularity = 8192, index_granularity_bytes = 65536, min_bytes_for_wide_part = 0;

INSERT INTO amt_granule
SELECT g, uniqExactState(v)
FROM (SELECT number % 2000 AS g, number AS v FROM numbers(1000000)) GROUP BY g;

-- Each granule must stay within the 64 KiB cap, and the part must be split into many granules
-- (without the fix the whole part is a single oversized granule).
SELECT
    max(data_uncompressed_bytes / marks) <= 65536 AS bytes_per_granule_within_cap,
    any(marks) > 10 AS split_into_many_granules
FROM system.parts WHERE table = 'amt_granule' AND active AND database = currentDatabase();

-- Data integrity is unaffected: every group still has exactly 500 distinct values.
SELECT min(u) = 500 AND max(u) = 500 AS all_groups_correct
FROM (SELECT g, uniqExactMerge(s) AS u FROM amt_granule GROUP BY g);

DROP TABLE IF EXISTS amt_granule;

-- Control: an ordinary String column of comparable per-row size already honors the cap.
DROP TABLE IF EXISTS str_granule;

CREATE TABLE str_granule (g UInt64, blob String)
ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 8192, index_granularity_bytes = 65536, min_bytes_for_wide_part = 0;

INSERT INTO str_granule SELECT number, repeat('x', 4096) FROM numbers(2000);

SELECT max(data_uncompressed_bytes / marks) <= 65536 AS bytes_per_granule_within_cap
FROM system.parts WHERE table = 'str_granule' AND active AND database = currentDatabase();

DROP TABLE IF EXISTS str_granule;
