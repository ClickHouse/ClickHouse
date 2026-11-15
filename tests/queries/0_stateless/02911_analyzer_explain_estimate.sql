-- Tags: distributed

SET enable_analyzer = 1;

EXPLAIN ESTIMATE SELECT 0 = 1048577, NULL, groupBitmapOr(bitmapBuild([toInt32(65537)])) FROM cluster(test_cluster_two_shards) WHERE NULL = 1048575;
