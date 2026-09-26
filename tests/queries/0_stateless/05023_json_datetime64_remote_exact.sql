-- Tags: no-parallel-replicas

-- A typed `DateTime64` leaf of a `JSON` constant must reach a remote shard as the exact instant.
-- A bare number there is read through `Float64` (losing digits past the 16th) and, under the legacy
-- `input_format_read_datetime_number_as_raw_value`, a bare integer is read as raw ticks.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

SELECT 'local, nanoseconds';
SELECT toUnixTimestamp64Nano(materialize(CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')).a) AS v
ORDER BY v;

SELECT 'remote, nanoseconds';
SELECT toUnixTimestamp64Nano(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v;

SELECT 'remote, whole seconds, legacy raw ticks';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.000"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS compatibility = '26.7';

SELECT 'remote, nanoseconds, best_effort';
SELECT toUnixTimestamp64Nano(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2023-10-29 01:30:00.123456789"}', 'JSON(a DateTime64(9, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort';

-- The best-effort parser does not read a negative timestamp from a string, so this leaf is sent as a number.
SELECT 'remote, before 1970, best_effort';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1969-12-31 23:59:58.5"}', 'JSON(a DateTime64(1, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort';
