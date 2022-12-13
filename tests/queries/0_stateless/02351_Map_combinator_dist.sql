-- Tags: no-backward-compatibility-check:22.6

-- https://github.com/ClickHouse/ClickHouse/issues/35359

-- sumMap
SELECT x[67]
FROM
(
    SELECT
        A,
        sumMap(CAST(arrayMap(x -> (x, 1), r), 'Map(UInt8,Int64)')) AS x
    FROM remote('127.{1,1}', view(
        SELECT
            number AS A,
            range(150) AS r
        FROM numbers(60)
        WHERE (A % 2) = shardNum()
    ))
    GROUP BY A
    LIMIT 100000000
)
WHERE A = 53
SETTINGS prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- minMap
SELECT x[0]
FROM
(
    SELECT
        A,
        minMap(CAST(arrayMap(x -> (x, 1), r), 'Map(UInt8,Int64)')) AS x
    FROM remote('127.{1,1}', view(
        SELECT
            number AS A,
            range(150) AS r
        FROM numbers(60)
        WHERE (A % 2) = shardNum()
    ))
    GROUP BY A
    LIMIT 100000000
)
WHERE A = 41
SETTINGS prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- maxMap
SELECT x[0]
FROM
(
    SELECT
        A,
        maxMap(CAST(arrayMap(x -> (x, 1), r), 'Map(UInt8,Int64)')) AS x
    FROM remote('127.{1,1}', view(
        SELECT
            number AS A,
            range(150) AS r
        FROM numbers(60)
        WHERE (A % 2) = shardNum()
    ))
    GROUP BY A
    LIMIT 100000000
)
WHERE A = 41
SETTINGS prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- avgMap
SELECT x[0]
FROM
(
    SELECT
        A,
        avgMap(CAST(arrayMap(x -> (x, 1), r), 'Map(UInt8,Int64)')) AS x
    FROM remote('127.{1,1}', view(
        SELECT
            number AS A,
            range(150) AS r
        FROM numbers(60)
        WHERE (A % 2) = shardNum()
    ))
    GROUP BY A
    LIMIT 100000000
)
WHERE A = 41
SETTINGS prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
