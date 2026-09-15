DROP TABLE IF EXISTS stream_rebuilt_projection_part_format;

CREATE TABLE stream_rebuilt_projection_part_format
(
    k UInt64,
    v UInt64,
    PROJECTION p_normal (SELECT k, v ORDER BY k),
    PROJECTION p_normal_compact (SELECT k, v ORDER BY k)
        WITH SETTINGS (min_rows_for_wide_part = 48)
)
ENGINE = MergeTree
ORDER BY (k, v)
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 24,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_part_format;

-- Keep source projections absent, then make the first rebuilt projection block
-- smaller than the final part while the full rebuilt projection exceeds the
-- table `Wide`-part threshold. The projection-local threshold also exercises
-- the direct `Compact` writer for the same rebuilt rows.
INSERT INTO stream_rebuilt_projection_part_format SELECT number, number FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 3, number + 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 6, number + 6 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 9, number + 9 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 12, number + 12 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 15, number + 15 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 18, number + 18 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 21, number + 21 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 24, number + 24 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 27, number + 27 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 30, number + 30 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_part_format SELECT number + 33, number + 33 FROM numbers(3);

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_part_format' AND active;

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_part_format FINAL;

SELECT part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_part_format' AND name = 'p_normal' AND active;
SELECT count() FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_part_format, p_normal);
SELECT part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_part_format' AND name = 'p_normal_compact' AND active;
SELECT count() FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_part_format, p_normal_compact);
CHECK TABLE stream_rebuilt_projection_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_part_format;
DROP TABLE IF EXISTS stream_rebuilt_projection_aggregate_part_format;

CREATE TABLE stream_rebuilt_projection_aggregate_part_format
(
    k UInt64,
    v UInt64,
    PROJECTION p_aggregate
    (
        SELECT
            sum(v) AS s01,
            sum(v + 1) AS s02,
            sum(v + 2) AS s03,
            sum(v + 3) AS s04,
            sum(v + 4) AS s05,
            sum(v + 5) AS s06,
            sum(v + 6) AS s07,
            sum(v + 7) AS s08,
            sum(v + 8) AS s09,
            sum(v + 9) AS s10,
            sum(v + 10) AS s11,
            sum(v + 11) AS s12,
            sum(v + 12) AS s13,
            sum(v + 13) AS s14,
            sum(v + 14) AS s15,
            sum(v + 15) AS s16,
            sum(v + 16) AS s17,
            sum(v + 17) AS s18,
            sum(v + 18) AS s19,
            sum(v + 19) AS s20,
            sum(v + 20) AS s21,
            sum(v + 21) AS s22,
            sum(v + 22) AS s23,
            sum(v + 23) AS s24,
            sum(v + 24) AS s25,
            sum(v + 25) AS s26,
            sum(v + 26) AS s27,
            sum(v + 27) AS s28,
            sum(v + 28) AS s29,
            sum(v + 29) AS s30,
            sum(v + 30) AS s31,
            sum(v + 31) AS s32
    )
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 10,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_aggregate_part_format;

INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (0, 1);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (1, 2);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (2, 3);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (3, 4);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (4, 5);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (5, 6);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (6, 7);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (7, 8);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (8, 9);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (9, 10);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (10, 11);
INSERT INTO stream_rebuilt_projection_aggregate_part_format VALUES (11, 12);

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_aggregate_part_format' AND active;

SYSTEM START MERGES stream_rebuilt_projection_aggregate_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_aggregate_part_format FINAL;

SELECT part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_aggregate_part_format' AND name = 'p_aggregate' AND active;
SELECT count(), sumMerge(`sum(v)`), sumMerge(`sum(plus(v, 31))`)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_aggregate_part_format, p_aggregate);
CHECK TABLE stream_rebuilt_projection_aggregate_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_aggregate_part_format;
DROP TABLE IF EXISTS stream_rebuilt_projection_grouped_aggregate_part_format;

CREATE TABLE stream_rebuilt_projection_grouped_aggregate_part_format
(
    k UInt64,
    v UInt64,
    PROJECTION p_aggregate (SELECT k, sum(v) GROUP BY k)
)
ENGINE = MergeTree
ORDER BY (k, v)
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 24,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_grouped_aggregate_part_format;

-- A grouped aggregate can emit several blocks even though the first one is
-- below the `Wide` threshold. Its projected row estimate must select `Wide`, while
-- the global aggregate above remains `Compact`.
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number, number FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 3, number + 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 6, number + 6 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 9, number + 9 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 12, number + 12 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 15, number + 15 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 18, number + 18 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 21, number + 21 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 24, number + 24 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 27, number + 27 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 30, number + 30 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_grouped_aggregate_part_format SELECT number + 33, number + 33 FROM numbers(3);

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_grouped_aggregate_part_format' AND active;

SYSTEM START MERGES stream_rebuilt_projection_grouped_aggregate_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_grouped_aggregate_part_format FINAL;

SELECT part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_grouped_aggregate_part_format' AND name = 'p_aggregate' AND active;
SELECT count(), sumMerge(`sum(v)`)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_grouped_aggregate_part_format, p_aggregate);
CHECK TABLE stream_rebuilt_projection_grouped_aggregate_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_grouped_aggregate_part_format;
