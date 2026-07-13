-- Regression test: out-of-bounds access in getRequiredHeaderPositions during
-- optimizeLazyMaterialization2 when a projection with PREWHERE is used.
-- The projection adds a _projection_filter column to the expression DAG,
-- making dag.getOutputs() larger than the required_output_positions vector
-- (sized from the sorting step's output header), causing an abort in debug builds.

DROP TABLE IF EXISTS test_lazy_mat_proj;
CREATE TABLE test_lazy_mat_proj
(
    id UInt64,
    event_date Date,
    url String,
    region String,
    PROJECTION region_url_proj
    (
        SELECT _part_offset ORDER BY region, url
    )
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO test_lazy_mat_proj VALUES (1, '2023-01-01', 'https://example.com/page1', 'europe');
INSERT INTO test_lazy_mat_proj VALUES (2, '2023-01-01', 'https://example.com/page2', 'us_west');

OPTIMIZE TABLE test_lazy_mat_proj FINAL;

-- The LIMIT triggers optimizeLazyMaterialization2, the projection provides
-- _projection_filter in PREWHERE, and ORDER BY ALL triggers the sorting step.
SELECT url FROM test_lazy_mat_proj WHERE region = 'europe' ORDER BY ALL LIMIT 10
SETTINGS query_plan_remove_unused_columns = 0, enable_multiple_prewhere_read_steps = 0,
    force_optimize_projection = 1, force_optimize_projection_name = 'region_url_proj';

DROP TABLE test_lazy_mat_proj;
