-- Test: statistics merging works with vertical insert.
SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS t_vi_stats_merging;

CREATE TABLE t_vi_stats_merging
(
    k UInt64,
    v UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    index_granularity = 1,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

SET materialize_statistics_on_insert = 1;

ALTER TABLE t_vi_stats_merging ADD STATISTICS k TYPE tdigest;

INSERT INTO t_vi_stats_merging
SELECT number, number % 10 FROM numbers(1000);

SELECT count()
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_vi_stats_merging'
  AND column = 'k'
  AND ifNull(length(statistics), 0) > 0
  AND active;

DROP TABLE t_vi_stats_merging;
