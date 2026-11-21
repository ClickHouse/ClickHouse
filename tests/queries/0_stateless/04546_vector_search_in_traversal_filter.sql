-- Tags: no-fasttest, no-ordinary-database
-- Verifies that row_bitmap filters can be consumed by vector_similarity exact search in the in_traversal path.
-- The expected rows are scalar-accepted nearest neighbours; ordinary postfilter with a small
-- candidate set would underfill this data layout because the nearest unfiltered rows have other groups.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab_in_traversal_filter;

CREATE TABLE tab_in_traversal_filter
(
    id UInt64,
    grp UInt8,
    vec Array(Float32),
    INDEX idx_grp grp TYPE row_bitmap GRANULARITY 1,
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 100;

INSERT INTO tab_in_traversal_filter
SELECT number, toUInt8(number % 100), [toFloat32(number), toFloat32(0)]
FROM numbers(1000);

SELECT 'in_traversal exact fallback returns scalar-accepted nearest rows';

SELECT id
FROM tab_in_traversal_filter
WHERE grp = 0
ORDER BY L2Distance(vec, [toFloat32(551), toFloat32(0)])
LIMIT 5
SETTINGS
    vector_search_filter_strategy = 'in_traversal',
    hnsw_candidate_list_size_for_search = 64;

DROP TABLE tab_in_traversal_filter;
