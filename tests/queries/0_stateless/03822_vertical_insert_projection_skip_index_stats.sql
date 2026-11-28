-- Tags: no-parallel-replicas
-- Test: projection + skip index + statistics work with vertical insert.
DROP TABLE IF EXISTS t_vi_proj_idx;

SET allow_experimental_statistics = 1;

CREATE TABLE t_vi_proj_idx
(
    id UInt64,
    v UInt64 STATISTICS(minmax),
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    PROJECTION p_sum
    (
        SELECT sum(v) AS s
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

INSERT INTO t_vi_proj_idx VALUES (1, 1), (2, 2), (3, 3);

ALTER TABLE t_vi_proj_idx MATERIALIZE STATISTICS v;

SELECT sum(v) AS s
FROM t_vi_proj_idx
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT count()
FROM system.data_skipping_indices
WHERE database = currentDatabase()
  AND table = 't_vi_proj_idx'
  AND name = 'idx_v';

SELECT position(statistics, 'minmax') > 0
FROM system.columns
WHERE database = currentDatabase()
  AND table = 't_vi_proj_idx'
  AND name = 'v';

DROP TABLE t_vi_proj_idx;
