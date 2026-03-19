-- Tags: no-parallel-replicas
-- Test: projection with TTL works with vertical insert.
DROP TABLE IF EXISTS t_vi_proj_ttl;

CREATE TABLE t_vi_proj_ttl
(
    d Date,
    v UInt64,
    PROJECTION p_cnt
    (
        SELECT d, count() AS c GROUP BY d
    )
)
ENGINE = MergeTree
ORDER BY d
TTL d + INTERVAL 1 DAY
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_proj_ttl VALUES
    (today() + 7, 1),
    (today() + 8, 2),
    (today() + 7, 3);

SELECT dateDiff('day', today(), d) AS d_offset, count() AS c
FROM t_vi_proj_ttl
GROUP BY d
ORDER BY d
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_vi_proj_ttl;
