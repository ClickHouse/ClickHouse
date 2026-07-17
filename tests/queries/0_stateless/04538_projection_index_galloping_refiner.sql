SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET use_projection_index_in_read_pools = 1;
SET enable_parallel_replicas = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;

DROP TABLE IF EXISTS t_projection_gallop;

CREATE TABLE t_projection_gallop
(
    id UInt64,
    region String,
    payload String,
    PROJECTION region_proj INDEX region TYPE basic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = 1024,
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_vertical_merge_algorithm = 0;

INSERT INTO t_projection_gallop
SELECT
    number,
    if(number IN (0, 42, 100, 101, 102, 2000), 'rare', 'common'),
    repeat('x', 8 + number % 64)
FROM numbers(2048);

INSERT INTO t_projection_gallop
SELECT
    2048 + number,
    if(2048 + number IN (2048, 3000, 4000, 4095), 'rare', 'common'),
    repeat('x', 8 + number % 64)
FROM numbers(2048);

OPTIMIZE TABLE t_projection_gallop FINAL;

-- Default pool with multiple readers and several separated primary-key ranges.
SELECT 'forward', groupArray(id)
FROM
(
    SELECT id
    FROM t_projection_gallop
    WHERE region = 'rare'
      AND ((id BETWEEN 0 AND 150) OR (id BETWEEN 1900 AND 3100) OR id >= 3900)
    ORDER BY id
    SETTINGS max_threads = 4, merge_tree_min_rows_for_concurrent_read = 128,
        merge_tree_min_read_task_size = 1, optimize_read_in_order = 0
);

-- In-order pool reuses a forward session across cuts.
SELECT 'limit_asc', groupArray(id)
FROM
(
    SELECT id
    FROM t_projection_gallop
    WHERE region = 'rare'
    ORDER BY id
    LIMIT 5
    SETTINGS max_threads = 1, optimize_read_in_order = 1
);

-- Reverse in-order pool uses predecessor searches over the same bitmap.
SELECT 'limit_desc', groupArray(id)
FROM
(
    SELECT id
    FROM t_projection_gallop
    WHERE region = 'rare'
    ORDER BY id DESC
    LIMIT 5
    SETTINGS max_threads = 1, optimize_read_in_order = 1
);

SELECT 'missing', count()
FROM t_projection_gallop
WHERE region = 'missing';

DROP TABLE t_projection_gallop;
