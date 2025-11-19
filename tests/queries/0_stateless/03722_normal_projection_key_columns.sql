SET enable_analyzer = 1;
-- enable projection for parallel replicas
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
-- avoid using projection index
SET min_table_rows_to_use_projection_index = 1000000;

CREATE TABLE test_projection
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

INSERT INTO test_projection VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe');
INSERT INTO test_projection VALUES (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west');

OPTIMIZE TABLE test_projection FINAL;

SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT url FROM test_projection WHERE region = 'europe')
WHERE explain LIKE '%ReadFromMergeTree%';
SELECT url FROM test_projection WHERE region = 'europe' ORDER BY ALL;
