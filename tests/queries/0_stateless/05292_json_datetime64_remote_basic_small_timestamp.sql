-- Tags: no-parallel-replicas

-- A typed `DateTime64` leaf of a `JSON` constant whose Unix timestamp has few digits of whole seconds, like
-- `0.001`, must reach a remote shard as the exact instant under `date_time_input_format = 'basic'`, which reads
-- such a number as a date. It used to throw a logical error found by the AST fuzzer.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;
SET date_time_input_format = 'basic';

SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1970-01-01 00:00:00.001"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v;

SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1969-12-31 23:59:59.999"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v;

SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1970-01-01 05:30:00.001"}', 'JSON(a DateTime64(3, \'Asia/Kolkata\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v;

-- The leaf of the query of the AST fuzzer.
SELECT toUnixTimestamp64Milli(json.a) = toUnixTimestamp64Milli(materialize(CAST('{"a":1}', 'JSON(a DateTime64(3, \'UTC\'))')).a)
FROM (SELECT materialize(CAST('{"a":1}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one));
