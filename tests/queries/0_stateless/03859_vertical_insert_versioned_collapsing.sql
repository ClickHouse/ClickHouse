-- Test: special engine columns for VersionedCollapsingMergeTree are handled during vertical insert.
DROP TABLE IF EXISTS t_vi_versioned_collapsing;

CREATE TABLE t_vi_versioned_collapsing
(
    k UInt64,
    sign Int8,
    version UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_versioned_collapsing VALUES
    (1, 1, 1),
    (1, -1, 2),
    (2, 1, 1);

SELECT sum(sign) FROM t_vi_versioned_collapsing FINAL;

DROP TABLE t_vi_versioned_collapsing;
