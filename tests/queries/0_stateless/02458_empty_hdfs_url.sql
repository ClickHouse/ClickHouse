-- Tags: no-fasttest, no-cpu-aarch64
SELECT * FROM hdfsCluster('test_cluster', '', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_cluster', ' ', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_cluster', '/', 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM hdfsCluster('test_cluster', 'http/', 'TSV'); -- { serverError BAD_ARGUMENTS }