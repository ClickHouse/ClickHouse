-- Tags: no-parallel-replicas
-- Test: projection uses DEFAULT expressions during vertical insert.
DROP TABLE IF EXISTS t_vi_proj_defaults;

CREATE TABLE t_vi_proj_defaults
(
    id UInt64,
    a UInt64,
    b UInt64 DEFAULT a * 10,
    PROJECTION p_default
    (
        SELECT id, sum(b) AS s GROUP BY id
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

INSERT INTO t_vi_proj_defaults (id, a) VALUES (1, 2), (2, 4);

SELECT id, sum(b) AS s
FROM t_vi_proj_defaults
GROUP BY id
ORDER BY id
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_vi_proj_defaults;
