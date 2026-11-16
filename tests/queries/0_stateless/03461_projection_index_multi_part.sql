-- Tags: no-random-settings, no-random-merge-tree-settings

-- Test for projection as secondary index with multiple parts
-- This is a regression test for https://github.com/ClickHouse/ClickHouse/issues/90953

SET enable_analyzer = 1;
SET optimize_use_projections = 1;
SET min_table_rows_to_use_projection_index = 0;

DROP TABLE IF EXISTS test_proj_multi_part;

CREATE TABLE test_proj_multi_part
(
    id UInt64,
    event_date Date,
    region String,
    value UInt32,
    PROJECTION by_date (
        SELECT _part_offset ORDER BY event_date
    )
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 16,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 0;

-- Insert data in multiple batches to create multiple parts (no OPTIMIZE)
INSERT INTO test_proj_multi_part SELECT number, toDate('2024-01-01') + (number % 100), 'region_' || toString(number % 7), number * 100 FROM numbers(1000);
INSERT INTO test_proj_multi_part SELECT number + 1000, toDate('2024-01-01') + (number % 100), 'region_' || toString(number % 7), number * 200 FROM numbers(1000);

-- Check that we have multiple parts
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_proj_multi_part' AND active;

-- Query with projection enabled should return same results as without projection
SELECT count() FROM test_proj_multi_part WHERE event_date = '2024-03-11' SETTINGS optimize_use_projections = 0, optimize_use_projection_filtering = 0;
SELECT count() FROM test_proj_multi_part WHERE event_date = '2024-03-11' SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1;

-- Also test with combined conditions
SELECT count() FROM test_proj_multi_part WHERE event_date = '2024-03-11' AND region = 'region_0' SETTINGS optimize_use_projections = 0, optimize_use_projection_filtering = 0;
SELECT count() FROM test_proj_multi_part WHERE event_date = '2024-03-11' AND region = 'region_0' SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1;

-- Test with SELECT *
SELECT * FROM test_proj_multi_part WHERE event_date = toDate('2024-02-10') AND id < 50 ORDER BY id SETTINGS optimize_use_projections = 0, optimize_use_projection_filtering = 0;
SELECT * FROM test_proj_multi_part WHERE event_date = toDate('2024-02-10') AND id < 50 ORDER BY id SETTINGS optimize_use_projections = 1, optimize_use_projection_filtering = 1;

DROP TABLE test_proj_multi_part;
