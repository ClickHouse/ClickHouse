CREATE TABLE test (dummy Int8) ENGINE = Distributed(test_cluster_two_shards, 'system', 'one');

SET allow_experimental_analyzer = 0;

SELECT * FROM (SELECT * FROM test SETTINGS allow_experimental_analyzer = 1); -- { serverError INCORRECT_QUERY }

SET allow_experimental_analyzer = 1;

SELECT * FROM (SELECT * FROM test SETTINGS allow_experimental_analyzer = 0); -- { serverError INCORRECT_QUERY }
