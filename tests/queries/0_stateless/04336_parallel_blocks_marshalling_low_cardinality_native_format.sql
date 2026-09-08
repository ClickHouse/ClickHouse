-- Tags: shard

-- With `low_cardinality_allow_in_native_format = 0` and parallel blocks marshalling enabled,
-- distributed queries over `LowCardinality` columns used to return corrupted data
-- or fail with `CANNOT_READ_ALL_DATA`, because the initiator received
-- `LowCardinality`-serialized data labeled as the underlying plain type.

SELECT toLowCardinality(toString(number % 3)) AS v, count()
FROM remote('127.0.0.{1,2}', numbers(100000))
GROUP BY v
ORDER BY v
SETTINGS low_cardinality_allow_in_native_format = 0, enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0;

SELECT sum(cityHash64(v)) FROM
(
    SELECT toLowCardinality(toString(number % 10)) AS v
    FROM remote('127.0.0.{1,2}', numbers(100000))
    ORDER BY v
)
SETTINGS low_cardinality_allow_in_native_format = 0, enable_parallel_blocks_marshalling = 1, prefer_localhost_replica = 0;
