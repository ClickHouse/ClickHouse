-- Tags: no-fasttest 
-- s3Cluster is not used in fast tests

CREATE TABLE test AS s3Cluster('test_shard_localhost', 'http://localhost:11111/test/a.tsv', 'TSV', 'a Int32'); -- { serverError BAD_ARGUMENTS }
