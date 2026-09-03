-- Here we use a trick with shardNum() to generate unique data on each shard.
-- Since distributed_group_by_no_merge=2 will use WithMergeableStateAfterAggregationAndLimit,
-- which assume that the data on shards is unique
-- (LIMIT BY will be applied only on shards, not on the initiator).

-- To distinguish echoing from the comments above we use SELECT FORMAT Null.
SELECT '' FORMAT Null;

-- { echo }
SELECT *
FROM remote('127.{1,2}', view(
    SELECT number%20 number
    FROM numbers(40)
    WHERE (number % 2) = (shardNum() - 1)
), number)
GROUP BY number
ORDER BY number ASC
LIMIT 1 BY number
LIMIT 5, 5
SETTINGS
    optimize_skip_unused_shards=1,
    optimize_distributed_group_by_sharding_key=1,
    distributed_push_down_limit=1;
SELECT *
FROM remote('127.{1,2}', view(
    SELECT number%20 number
    FROM numbers(40)
    WHERE (number % 2) = (shardNum() - 1)
), number)
GROUP BY number
ORDER BY number ASC
LIMIT 1 BY number
LIMIT 5, 5
SETTINGS
    distributed_group_by_no_merge=2,
    distributed_push_down_limit=1;
