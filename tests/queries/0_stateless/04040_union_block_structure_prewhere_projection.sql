-- Regression test for Block structure mismatch in UnionStep with PREWHERE and projections.
-- The debug assertion in UnionStep::updatePipeline fired before the header conversion code
-- could fix the mismatch, causing an exception in debug/sanitizer builds.
-- Triggered by PREWHERE optimization adding extra pass-through columns to ReadFromMergeTree
-- output that are not consumed by the expression DAG above.
-- https://github.com/ClickHouse/ClickHouse/issues/96131

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET query_plan_remove_unused_columns = 0;
SET min_table_rows_to_use_projection_index = 1000000;

DROP TABLE IF EXISTS test_union_prewhere_proj;
CREATE TABLE test_union_prewhere_proj
(
    id UInt64,
    event_date Date,
    user_id UInt32,
    url String,
    region String,
    PROJECTION region_url_proj
    (
        SELECT _part_offset ORDER BY region, url
    )
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS
    index_granularity = 1, min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, enable_vertical_merge_algorithm = 0;

INSERT INTO test_union_prewhere_proj VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe');
INSERT INTO test_union_prewhere_proj VALUES (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west');

OPTIMIZE TABLE test_union_prewhere_proj FINAL;

SELECT url FROM test_union_prewhere_proj WHERE region = 'europe' ORDER BY ALL;

DROP TABLE test_union_prewhere_proj;
