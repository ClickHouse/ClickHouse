-- Tags: no-fasttest
SELECT * FROM hdfsCluster('test_shard_localhost', '', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_shard_localhost', ' ', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_shard_localhost', '/', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_shard_localhost', 'http/', 'TSV'); -- { serverError BAD_ARGUMENTS }
