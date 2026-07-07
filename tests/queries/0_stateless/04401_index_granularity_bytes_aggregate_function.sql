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

-- An AggregateFunction leaf nested inside a Tuple must be sized the same way: the granularity
-- accountant recurses through wrappers (Tuple/Array/Map/...) and corrects every nested leaf.
DROP TABLE IF EXISTS amt_nested;

CREATE TABLE amt_nested (g UInt64, s Tuple(AggregateFunction(uniqExact, UInt64)))
ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 8192, index_granularity_bytes = 65536, min_bytes_for_wide_part = 0;

INSERT INTO amt_nested
SELECT g, tuple(uniqExactState(v))
FROM (SELECT number % 2000 AS g, number AS v FROM numbers(1000000)) GROUP BY g;

SELECT
    max(data_uncompressed_bytes / marks) <= 65536 AS bytes_per_granule_within_cap,
    any(marks) > 10 AS split_into_many_granules
FROM system.parts WHERE table = 'amt_nested' AND active AND database = currentDatabase();

SELECT min(u) = 500 AND max(u) = 500 AS all_groups_correct
FROM (SELECT g, uniqExactMerge(s.1) AS u FROM amt_nested GROUP BY g);

DROP TABLE IF EXISTS amt_nested;

-- Skewed state sizes (most groups tiny, one group huge) must also be split. The size must come from the
-- exact serialized size of every state, not from sampled states: the single huge state below sits at row 1
-- (g = 2, after ORDER BY g), which no strided sampler hits, so a sampling estimate would miss it and keep
-- one oversized granule.
DROP TABLE IF EXISTS amt_skew;

CREATE TABLE amt_skew (g UInt64, s AggregateFunction(uniqExact, UInt64))
ENGINE = AggregatingMergeTree ORDER BY g
SETTINGS index_granularity = 8192, index_granularity_bytes = 65536, min_bytes_for_wide_part = 0;

INSERT INTO amt_skew
SELECT g, uniqExactState(v)
FROM
(
    SELECT 2 AS g, number AS v FROM numbers(300000)
    UNION ALL
    SELECT number AS g, number AS v FROM numbers(1, 2000)
) GROUP BY g;

SELECT
    max(data_uncompressed_bytes / marks) <= 65536 AS bytes_per_granule_within_cap,
    any(marks) > 1 AS split_into_many_granules
FROM system.parts WHERE table = 'amt_skew' AND active AND database = currentDatabase();

SELECT
    (SELECT uniqExactMerge(s) FROM amt_skew WHERE g = 2) = 300000
    AND (SELECT max(u) FROM (SELECT uniqExactMerge(s) AS u FROM amt_skew WHERE g != 2 GROUP BY g)) = 1
        AS skewed_groups_correct;

DROP TABLE IF EXISTS amt_skew;

-- An aggregating projection writes its own AggregateFunction state column, through a separate
-- write-path granularity site, so it must honor index_granularity_bytes too. use_const_adaptive_granularity
-- forces that site to size the projection part (otherwise the per-block adaptive site recomputes it),
-- so a regression there (e.g. reverting to Block::bytes()) keeps one oversized projection granule.
DROP TABLE IF EXISTS amt_proj;

CREATE TABLE amt_proj
(
    g UInt64,
    v UInt64,
    PROJECTION p (SELECT g, uniqExactState(v) GROUP BY g)
)
ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 8192, index_granularity_bytes = 65536, min_bytes_for_wide_part = 0,
         materialize_projections_on_insert = 1, use_const_adaptive_granularity = 1;

INSERT INTO amt_proj SELECT number % 2000 AS g, number AS v FROM numbers(1000000);

SELECT
    max(data_uncompressed_bytes / marks) <= 65536 AS bytes_per_granule_within_cap,
    any(marks) > 10 AS split_into_many_granules
FROM system.projection_parts
WHERE table = 'amt_proj' AND active AND database = currentDatabase();

SELECT min(u) = 500 AND max(u) = 500 AS all_groups_correct
FROM (SELECT g, uniqExact(v) AS u FROM amt_proj GROUP BY g);

DROP TABLE IF EXISTS amt_proj;

-- Control: an ordinary String column of comparable per-row size already honors the cap.
DROP TABLE IF EXISTS str_granule;

CREATE TABLE str_granule (g UInt64, blob String)
ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 8192, index_granularity_bytes = 65536, min_bytes_for_wide_part = 0;

INSERT INTO str_granule SELECT number, repeat('x', 4096) FROM numbers(2000);

SELECT max(data_uncompressed_bytes / marks) <= 65536 AS bytes_per_granule_within_cap
FROM system.parts WHERE table = 'str_granule' AND active AND database = currentDatabase();

DROP TABLE IF EXISTS str_granule;
