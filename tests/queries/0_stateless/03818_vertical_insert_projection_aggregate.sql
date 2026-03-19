-- Tags: no-parallel-replicas
-- Test: aggregation projections (with aliases) are built during vertical insert.
DROP TABLE IF EXISTS t_vi_proj_agg;

CREATE TABLE t_vi_proj_agg
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

INSERT INTO t_vi_proj_agg VALUES (1, 10), (1, 20), (2, 5);

SELECT id, sum(v) AS s
FROM t_vi_proj_agg
GROUP BY id
ORDER BY id
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_vi_proj_agg;
