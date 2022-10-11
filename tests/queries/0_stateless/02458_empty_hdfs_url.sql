-- Tags: no-fasttest
SELECT * FROM hdfsCluster('test_cluster', '', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_cluster', ' ', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_cluster', '/', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_cluster', 'http/', 'TSV'); -- { serverError BAD_ARGUMENTS }