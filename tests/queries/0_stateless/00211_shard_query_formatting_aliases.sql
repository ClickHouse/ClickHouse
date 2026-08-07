-- Tags: shard

SELECT toUInt64(1) IN (1234567890, 2345678901, 3456789012, 4567890123, 5678901234, 6789012345, 7890123456, 8901234567, 9012345678, 123456789) AS x,
    x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
    x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
    x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x,
    x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x, x
FROM remote('127.0.0.2', system, one) SETTINGS max_query_size = 10000;

SELECT 1 AS x, x, (SELECT 2 AS x, x) FROM remote('127.0.0.{2,3}', system.one) WHERE (3, 4) IN (SELECT 3 AS x, toUInt8(x + 1));
