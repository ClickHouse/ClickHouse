-- Regression test: reading a Compact part where a whole granule of a nested JSON column has 0
-- shared-data rows (all arrays empty) used to leave the empty chunk's structure bytes unconsumed,
-- desyncing a trailing substream read in the same column (here Tuple element `b`).

DROP TABLE IF EXISTS t_adv_marks;
DROP TABLE IF EXISTS t_adv_nomarks;
DROP TABLE IF EXISTS t_advchunked_marks;
DROP TABLE IF EXISTS t_advchunked_nomarks;

CREATE TABLE t_adv_marks (t Tuple(a Array(JSON(max_dynamic_paths=0)), b UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 1,
         object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced';

CREATE TABLE t_adv_nomarks (t Tuple(a Array(JSON(max_dynamic_paths=0)), b UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 0,
         object_shared_data_serialization_version = 'advanced',
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced';

CREATE TABLE t_advchunked_marks (t Tuple(a Array(JSON(max_dynamic_paths=0)), b UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 1,
         object_shared_data_serialization_version = 'advanced_chunked', object_shared_data_target_chunk_rows = 2,
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced_chunked';

CREATE TABLE t_advchunked_nomarks (t Tuple(a Array(JSON(max_dynamic_paths=0)), b UInt64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
         write_marks_for_substreams_in_compact_parts = 0,
         object_shared_data_serialization_version = 'advanced_chunked', object_shared_data_target_chunk_rows = 2,
         object_shared_data_serialization_version_for_zero_level_parts = 'advanced_chunked';

-- Granule 0 (rows 0-3): all `a` arrays empty -> nested JSON has 0 shared-data rows.
-- Granule 1 (rows 4-7): non-empty `a`. `b` is non-zero in every row.
INSERT INTO t_adv_marks
SELECT tuple(if(number < 4, [], [toJSONString(map('k' || toString(number), number))]::Array(JSON(max_dynamic_paths=0))), 100 + number)
FROM numbers(8);
INSERT INTO t_adv_nomarks SELECT * FROM t_adv_marks;
INSERT INTO t_advchunked_marks SELECT * FROM t_adv_marks;
INSERT INTO t_advchunked_nomarks SELECT * FROM t_adv_marks;

SELECT 'advanced marks';
SELECT t.b FROM t_adv_marks ORDER BY t.b;
SELECT t FROM t_adv_marks ORDER BY t.b;

SELECT 'advanced nomarks';
SELECT t.b FROM t_adv_nomarks ORDER BY t.b;
SELECT t FROM t_adv_nomarks ORDER BY t.b;

SELECT 'advanced_chunked marks';
SELECT t.b FROM t_advchunked_marks ORDER BY t.b;
SELECT t FROM t_advchunked_marks ORDER BY t.b;

SELECT 'advanced_chunked nomarks';
SELECT t.b FROM t_advchunked_nomarks ORDER BY t.b;
SELECT t FROM t_advchunked_nomarks ORDER BY t.b;

DROP TABLE t_adv_marks;
DROP TABLE t_adv_nomarks;
DROP TABLE t_advchunked_marks;
DROP TABLE t_advchunked_nomarks;
