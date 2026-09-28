-- `toStartOfMonth` over a sparse column returns a full column for a chunk without default rows and a sparse one otherwise,
-- so a descending read in order passes FinishSortingTransform a full chunk followed by a sparse one.

DROP TABLE IF EXISTS t_finish_sorting_sparse;

CREATE TABLE t_finish_sorting_sparse (date Date, i UInt64)
ENGINE = MergeTree ORDER BY (date, i)
SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.9, min_bytes_for_wide_part = 0;

INSERT INTO t_finish_sorting_sparse SELECT if(number < 4750, toDate(0), toDate('2020-10-10')), number FROM numbers(5000);

SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_finish_sorting_sparse' AND column = 'date' AND active;

SELECT toStartOfMonth(date) AS d, i FROM t_finish_sorting_sparse ORDER BY d DESC, i LIMIT 5 SETTINGS optimize_read_in_order = 1;

DROP TABLE t_finish_sorting_sparse;
