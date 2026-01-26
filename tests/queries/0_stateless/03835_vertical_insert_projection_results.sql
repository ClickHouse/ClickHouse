-- Tags: no-parallel-replicas
-- Test: projection query results are correct after vertical insert.
DROP TABLE IF EXISTS t_vi_proj_results;

CREATE TABLE t_vi_proj_results
(
    k UInt64,
    v UInt64,
    PROJECTION p_sum
    (
        SELECT k, sum(v) AS s GROUP BY k
    )
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_proj_results VALUES (1, 10), (1, 20), (2, 7), (2, 3);

SELECT k, sum(v) AS s
FROM t_vi_proj_results
WHERE k IN (1, 2)
GROUP BY k
ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_vi_proj_results;
