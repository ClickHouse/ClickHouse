-- Test: partition key columns are handled in phase-1 when vertical insert is enabled.
DROP TABLE IF EXISTS t_vi_partition;

CREATE TABLE t_vi_partition
(
    k UInt64,
    d Date,
    v UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_partition VALUES
    (1, toDate('2020-01-01'), 1),
    (2, toDate('2020-01-15'), 2),
    (3, toDate('2020-02-01'), 3),
    (4, toDate('2020-02-15'), 4);

SELECT countDistinct(partition_id)
FROM system.parts
WHERE database = currentDatabase()
  AND table = 't_vi_partition'
  AND active;

SELECT sum(v) FROM t_vi_partition;

DROP TABLE t_vi_partition;
