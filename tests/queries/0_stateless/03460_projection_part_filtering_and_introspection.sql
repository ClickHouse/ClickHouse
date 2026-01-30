-- { echo ON }

DROP TABLE IF EXISTS test_simple_projection;

CREATE TABLE test_simple_projection
(
    id UInt64,
    event_date Date,
    user_id UInt32,
    url String,
    region String,
    PROJECTION region_proj
    (
        SELECT _part_offset ORDER BY region
    ),
    PROJECTION user_id_proj
    (
        SELECT _part_offset ORDER BY user_id
    )
)
ENGINE = MergeTree
ORDER BY (event_date, id)
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1; -- disable merge

INSERT INTO test_simple_projection VALUES (1, '2023-01-01', 101, 'https://example.com/page1', 'europe');
INSERT INTO test_simple_projection VALUES (2, '2023-01-01', 102, 'https://example.com/page2', 'us_west');
INSERT INTO test_simple_projection VALUES (3, '2023-01-02', 106, 'https://example.com/page3', 'us_west');
INSERT INTO test_simple_projection VALUES (4, '2023-01-02', 107, 'https://example.com/page4', 'us_west');
INSERT INTO test_simple_projection VALUES (5, '2023-01-03', 104, 'https://example.com/page5', 'asia');

SET enable_analyzer = 1;
SET optimize_use_projection_filtering = 1;
-- enable projection for parallel replicas
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

-- region projection is enough effective for filtering
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region = 'europe' AND user_id = 101)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

-- Only user_id projection is effective for filtering
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region != 'unknown' AND user_id = 106)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

-- Both region and user_id projections are effective for filtering
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region = 'us_west' AND user_id = 107)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

-- Neither projection is effective for filtering
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT * FROM test_simple_projection WHERE region != 'unknown' AND user_id != 999)
WHERE explain LIKE '%ReadFromMergeTree%' OR match(explain, '^\s+[A-Z][a-z]+(\s+[A-Z][a-z]+)*:');

DROP TABLE test_simple_projection;
