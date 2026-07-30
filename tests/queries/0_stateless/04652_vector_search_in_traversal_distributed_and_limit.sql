-- Tags: no-fasttest, no-ordinary-database
-- Regression coverage for https://github.com/ClickHouse/ClickHouse/pull/112406.
SET allow_experimental_parallel_reading_from_replicas = 0;
SET enable_analyzer = 1;
SET send_logs_level = 'error';

DROP TABLE IF EXISTS vector_in_traversal_dia;

CREATE TABLE vector_in_traversal_dia
(
    id UInt64,
    grp UInt8,
    vec Array(Float32),
    INDEX idx_grp grp TYPE row_bitmap GRANULARITY 100,
    INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    distributed_index_analysis_min_parts_to_activate = 0,
    distributed_index_analysis_min_indexes_bytes_to_activate = 0;

INSERT INTO vector_in_traversal_dia
SELECT number, toUInt8(number % 100), [toFloat32(number), toFloat32(0)]
FROM numbers(1000);

SELECT 'exact in_traversal keeps SQL LIMIT with a fetch multiplier';

SELECT 1
FROM vector_in_traversal_dia
WHERE grp = 0
ORDER BY L2Distance(vec, [toFloat32(551), toFloat32(0)])
LIMIT 5
SETTINGS
    vector_search_filter_strategy = 'in_traversal',
    vector_search_index_fetch_multiplier = 10,
    log_comment = '04652_in_traversal_limit'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04652_in_traversal_limit'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT 'distributed index analysis preserves in_traversal';

SELECT id
FROM vector_in_traversal_dia
WHERE grp = 0
ORDER BY L2Distance(vec, [toFloat32(551), toFloat32(0)])
LIMIT 5
SETTINGS
    vector_search_filter_strategy = 'in_traversal',
    distributed_index_analysis_for_non_shared_merge_tree = 1,
    distributed_index_analysis = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'parallel_replicas',
    log_comment = '04652_in_traversal_dia';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04652_in_traversal_dia'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE vector_in_traversal_dia;
