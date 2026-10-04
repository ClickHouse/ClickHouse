-- Subcolumn reads from a table holding Compact parts written both before and after
-- `write_marks_for_substreams_in_compact_parts` was enabled, as happens after an upgrade.

DROP TABLE IF EXISTS t_mixed_substream_marks;

CREATE TABLE t_mixed_substream_marks (k UInt64, t Tuple(a UInt32, b String), arr Array(UInt32))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
         index_granularity = 128, index_granularity_bytes = '100Gi',
         ratio_of_defaults_for_sparse_serialization = 1.0,
         write_marks_for_substreams_in_compact_parts = 0;

SYSTEM STOP MERGES t_mixed_substream_marks;

INSERT INTO t_mixed_substream_marks
SELECT number, (number, toString(number)), range(number % 5) FROM numbers(500)
SETTINGS max_insert_threads = 1;

ALTER TABLE t_mixed_substream_marks MODIFY SETTING write_marks_for_substreams_in_compact_parts = 1;

INSERT INTO t_mixed_substream_marks
SELECT number, (number, toString(number)), range(number % 5) FROM numbers(500, 500)
SETTINGS max_insert_threads = 1;

SELECT part_type, count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_mixed_substream_marks' AND active
GROUP BY part_type ORDER BY part_type;

-- Only the second part has per-substream marks, so the two parts take different reader branches.
SELECT part_name,
       any(isNotNull((`t.mark`).offset_in_compressed_file)) AS whole_column_mark,
       any(isNotNull((`arr.size0.mark`).offset_in_compressed_file)) AS substream_mark
FROM mergeTreeIndex(currentDatabase(), t_mixed_substream_marks, with_marks = 1)
GROUP BY part_name ORDER BY part_name;

-- `k` < 500 is exactly the rows of the first part, merges are stopped.
SELECT k < 500 AS in_first_part, sum(t.a), sum(length(t.b)), sum(arr.size0)
FROM t_mixed_substream_marks GROUP BY in_first_part ORDER BY in_first_part;

SELECT k, t.a, t.b, arr.size0 FROM t_mixed_substream_marks WHERE k IN (0, 499, 500, 999) ORDER BY k;

DROP TABLE t_mixed_substream_marks;
