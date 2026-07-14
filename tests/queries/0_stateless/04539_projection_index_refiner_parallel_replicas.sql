SET optimize_use_projections = 1, optimize_use_projection_filtering = 1;
SET min_table_rows_to_use_projection_index = 0;
SET use_projection_index_in_read_pools = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, enable_parallel_replicas = 1;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET send_logs_level = 'error';

DROP TABLE IF EXISTS t_projection_refiner_parallel;

CREATE TABLE t_projection_refiner_parallel
(
    id UInt64,
    region String,
    PROJECTION region_proj INDEX region TYPE basic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 16;

INSERT INTO t_projection_refiner_parallel
SELECT
    number,
    if(number IN (42, 100, 2000, 4000), 'rare', 'common')
FROM numbers(4096);

SELECT 'default', count(), sum(id)
FROM t_projection_refiner_parallel
WHERE region = 'rare'
SETTINGS optimize_read_in_order = 0;

SELECT 'ascending', groupArray(id)
FROM
(
    SELECT id
    FROM t_projection_refiner_parallel
    WHERE region = 'rare'
    ORDER BY id
    LIMIT 3
);

SELECT 'descending', groupArray(id)
FROM
(
    SELECT id
    FROM t_projection_refiner_parallel
    WHERE region = 'rare'
    ORDER BY id DESC
    LIMIT 3
);

DROP TABLE t_projection_refiner_parallel;
