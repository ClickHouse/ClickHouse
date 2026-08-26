-- Tags: no-parallel-replicas

-- Test: a typed DateTime64 leaf of a JSON constant must reach a remote shard as the exact instant.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

SELECT toUnixTimestamp64Nano(materialize(CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')).a) AS v
ORDER BY v;

SELECT toUnixTimestamp64Nano(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v;

SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.000"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS compatibility = '26.7';
