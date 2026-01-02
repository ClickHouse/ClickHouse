-- Test: projections with parallel replicas work after vertical insert.
DROP TABLE IF EXISTS t_vi_proj_pr;

CREATE TABLE t_vi_proj_pr
(
    id UInt64,
    v UInt64,
    PROJECTION p_sum
    (
        SELECT id, sum(v) AS s GROUP BY id
    )
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_proj_pr VALUES (1, 10), (1, 20), (2, 5);

SET enable_parallel_replicas = 1,
    parallel_replicas_local_plan = 1,
    parallel_replicas_support_projection = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    optimize_aggregation_in_order = 0;

SELECT id, sum(v) AS s
FROM t_vi_proj_pr
GROUP BY id
ORDER BY id
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_vi_proj_pr;
