-- `File` clamps its actual source count to its paths. A `Merge` read must let
-- that child-side reduction happen instead of rejecting the raw stream request.
DROP TABLE IF EXISTS t_merge_file_num_streams_limit;
DROP TABLE IF EXISTS t_file_num_streams_limit;

CREATE TABLE t_file_num_streams_limit (n UInt64)
ENGINE = File(TabSeparated, 'merge_file_num_streams_limit.tsv');
INSERT INTO t_file_num_streams_limit VALUES (1);

CREATE TABLE t_merge_file_num_streams_limit (n UInt64)
ENGINE = Merge(currentDatabase(), '^t_file_num_streams_limit$');

SELECT count() FROM t_merge_file_num_streams_limit
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1073741824;

DROP TABLE t_merge_file_num_streams_limit;
DROP TABLE t_file_num_streams_limit;
