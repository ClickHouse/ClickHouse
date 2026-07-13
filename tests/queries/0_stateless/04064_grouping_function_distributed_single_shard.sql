-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/71270
-- The `grouping` function on a Distributed table with a single shard used to
-- fail with "Method executeImpl is not supported for 'grouping' function".

-- Disable serialize_query_plan because the specialized grouping functions
-- (groupingForGroupingSets, etc.) are not registered in the FunctionFactory
-- and cannot be deserialized on the shard.
SET serialize_query_plan = 0;

-- Part 1: Tests using remote() table function (exercises ClusterProxy code path).

SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.1', numbers(10))
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;

-- Same query with prefer_localhost_replica=0 (forces remote execution path).
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM remote('127.0.0.1', numbers(10))
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr
SETTINGS prefer_localhost_replica = 0;

-- ROLLUP with grouping function via remote().
SELECT
    number,
    grouping(number) AS gr
FROM remote('127.0.0.1', numbers(5))
GROUP BY ROLLUP(number)
ORDER BY number, gr;

-- CUBE with grouping function via remote().
SELECT
    number % 2 AS k1,
    number % 3 AS k2,
    grouping(k1, k2) AS gr
FROM remote('127.0.0.1', numbers(6))
GROUP BY CUBE(k1, k2)
ORDER BY k1, k2, gr;

-- Part 2: Tests using a real Distributed table (exercises StorageDistributed::read path).

DROP TABLE IF EXISTS local_t;
DROP TABLE IF EXISTS dist_t;

CREATE TABLE local_t (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO local_t SELECT number FROM numbers(10);

CREATE TABLE dist_t AS local_t ENGINE = Distributed(test_shard_localhost, currentDatabase(), local_t);

-- GROUPING SETS via Distributed table.
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM dist_t
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr;

-- GROUPING SETS via Distributed table with prefer_localhost_replica=0.
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM dist_t
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr
SETTINGS prefer_localhost_replica = 0;

-- ROLLUP via Distributed table.
SELECT
    number,
    grouping(number) AS gr
FROM dist_t
WHERE number < 5
GROUP BY ROLLUP(number)
ORDER BY number, gr;

-- CUBE via Distributed table.
SELECT
    number % 2 AS k1,
    number % 3 AS k2,
    grouping(k1, k2) AS gr
FROM dist_t
WHERE number < 6
GROUP BY CUBE(k1, k2)
ORDER BY k1, k2, gr;

-- Part 3: Test with parallel replicas (exercises executeQueryWithParallelReplicas code path).
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM dist_t
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr
SETTINGS enable_analyzer = 1, allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Part 4: Test with parallel replicas and serialize_query_plan = 1
-- (exercises buildQueryPlanForParallelReplicas / findParallelReplicasQuery code path,
-- which is only reached when serialize_query_plan is enabled).
SELECT
    number,
    grouping(number, number % 2) AS gr
FROM dist_t
GROUP BY
    GROUPING SETS (
        (number),
        (number % 2)
    )
ORDER BY number, gr
SETTINGS enable_analyzer = 1, serialize_query_plan = 1, allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

DROP TABLE dist_t;
DROP TABLE local_t;
