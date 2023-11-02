SELECT count() FROM cluster('test_cluster_two_shards', view( SELECT * FROM numbers(100000000000) )) SETTINGS max_execution_time_leaf = 1; -- { serverError 159 }
-- Can return partial result
SELECT IF(count() > 0, 'OK', 'FAIL') FROM cluster('test_cluster_two_shards', view( SELECT * FROM numbers(100000000000) )) SETTINGS max_execution_time_leaf = 1, timeout_overflow_mode_leaf = 'break';
