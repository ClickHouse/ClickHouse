-- Test: multiple parts merged after vertical insert produce correct results post-OPTIMIZE.
DROP TABLE IF EXISTS t_vi_merge_parts;

CREATE TABLE t_vi_merge_parts
(
    k UInt64,
    v UInt64
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

SYSTEM STOP MERGES t_vi_merge_parts;

INSERT INTO t_vi_merge_parts VALUES
    (1, 10),
    (2, 20);

INSERT INTO t_vi_merge_parts VALUES
    (3, 30),
    (4, 40);

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
    AND table = 't_vi_merge_parts'
    AND active = 1;

SYSTEM START MERGES t_vi_merge_parts;

OPTIMIZE TABLE t_vi_merge_parts FINAL;

SELECT count()
FROM system.parts
WHERE database = currentDatabase()
    AND table = 't_vi_merge_parts'
    AND active = 1;

SELECT sum(v) FROM t_vi_merge_parts;

DROP TABLE t_vi_merge_parts;
