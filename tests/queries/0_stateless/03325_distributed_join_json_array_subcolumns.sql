SET enable_json_type=1;
SET allow_experimental_analyzer=1;

DROP TABLE IF EXISTS test_distr;
DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    id Int64,
    data JSON(arr1 Array(String), arr2 Array(Int32))
)
ENGINE = MergeTree ORDER BY id;


CREATE TABLE test_distr
(
    id Int64,
    data JSON(arr1 Array(String), arr2 Array(Int32))
)
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), 'test', murmurHash2_32(id));

INSERT INTO test FORMAT Values (1, '{"arr1" : ["s1", "s2", "s3"], "arr2" : []}'), (2, '{"arr1" : ["s4", "s5"], "arr2" : [42]}');

SELECT count()
FROM test_distr as left
GLOBAL INNER JOIN test_distr as right on left.id = right.id
WHERE has(right.data.arr1, 's3') AND has(right.data.arr2, 42) settings serialize_query_plan = 0;

SELECT count()
FROM test_distr as left
GLOBAL INNER JOIN test_distr as right on left.id = right.id
WHERE has(right.data.arr1, 's3') AND has(right.data.arr2, 42) settings enable_parallel_replicas = 0;

DROP TABLE test_distr;
DROP TABLE test;
