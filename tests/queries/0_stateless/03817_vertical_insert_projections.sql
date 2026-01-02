-- Test: projections are built correctly with vertical insert.
DROP TABLE IF EXISTS t_vi_proj;

CREATE TABLE t_vi_proj
(
    id UInt64,
    v UInt64,
    PROJECTION p_sum
    (
        SELECT id, sum(v) GROUP BY id
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

INSERT INTO t_vi_proj VALUES (1, 10), (1, 20), (2, 5);

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_vi_proj'
  AND active
  AND notEmpty(projections);

DROP TABLE t_vi_proj;
