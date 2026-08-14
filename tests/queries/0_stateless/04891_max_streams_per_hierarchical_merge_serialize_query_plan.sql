-- Tags: shard

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT number
FROM remote('127.0.0.1', numbers(10))
ORDER BY number
SETTINGS max_streams_per_hierarchical_merge = 1; -- { serverError BAD_ARGUMENTS }
