-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SELECT * FROM s3(headers('random_header' = 'value')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM s3Cluster('test_cluster_one_shard_two_replicas', headers('random_header' = 'value')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
