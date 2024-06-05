-- Tags: no-fasttest
SELECT count() FROM cluster('test_cluster_two_shards', view( SELECT * FROM numbers(100000000000) )) SETTINGS max_execution_time_leaf = 1; -- { serverError TIMEOUT_EXCEEDED }
-- Can return partial result
SELECT count() FROM cluster('test_cluster_two_shards', view( SELECT * FROM numbers(100000000000) )) FORMAT Null SETTINGS max_execution_time_leaf = 1, timeout_overflow_mode_leaf = 'break';
