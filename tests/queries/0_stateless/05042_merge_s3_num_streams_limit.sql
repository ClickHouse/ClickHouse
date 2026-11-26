-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- `S3` with a static key list clamps its actual source count to the key count.
-- A `Merge` read must let that child-side reduction happen instead of rejecting
-- the raw stream request.

DROP TABLE IF EXISTS t_merge_s3_num_streams_limit;
DROP TABLE IF EXISTS t_s3_num_streams_limit;

CREATE TABLE t_s3_num_streams_limit (name String, number UInt32)
ENGINE = S3('http://localhost:11111/test/tsv_with_header.tsv', 'test', 'testtest', 'TSVWithNames');

CREATE TABLE t_merge_s3_num_streams_limit (name String, number UInt32)
ENGINE = Merge(currentDatabase(), '^t_s3_num_streams_limit$');

SELECT count() FROM t_merge_s3_num_streams_limit
SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1073741824;

DROP TABLE t_merge_s3_num_streams_limit;
DROP TABLE t_s3_num_streams_limit;
