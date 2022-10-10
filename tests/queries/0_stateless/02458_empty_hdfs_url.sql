-- Tags: no-fasttest
SELECT * FROM hdfsCluster('test_cluster', '', 'TSV'); -- { serverError BAD_ARGUMENTS }