-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM s3(headers('random_header' = 'value')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM s3Cluster('test_cluster_two_shards_localhost', headers('random_header' = 'value')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SET enable_analyzer = 1;
EXPLAIN QUERY TREE SELECT 1 FROM s3('a', 1, CSV); -- { serverError BAD_ARGUMENTS }
