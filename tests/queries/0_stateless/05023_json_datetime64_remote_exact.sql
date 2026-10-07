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

SELECT 'remote, before 1970, best_effort';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1969-12-31 23:59:58.5"}', 'JSON(a DateTime64(1, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort';

-- The best-effort parsers read a Unix timestamp only with 9 or 10 digits of whole seconds, so a leaf before
-- 1973-03-03 or after 2286-11-20 is sent in the form that the `date_time_input_format` of the query reads.
-- A bare number would be rounded through `Float64`, or read as raw ticks under `compatibility = '26.7'`.

SELECT 'remote, after 2286, fractional, basic';
SELECT toUnixTimestamp64Micro(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2286-11-20 17:46:40.123456"}', 'JSON(a DateTime64(6, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'basic', compatibility = '26.7';

SELECT 'remote, after 2286, fractional, Europe/Berlin, basic';
SELECT toUnixTimestamp64Micro(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2286-11-20 18:46:40.123456"}', 'JSON(a DateTime64(6, \'Europe/Berlin\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'basic', compatibility = '26.7';

SELECT 'remote, before 1970, whole seconds, basic';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1969-12-31 23:59:58.000"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'basic', compatibility = '26.7';

SELECT 'remote, in 1900, fractional, basic';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1900-01-01 00:00:00.5"}', 'JSON(a DateTime64(1, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'basic', compatibility = '26.7';

SELECT 'remote, after 2286, fractional, best_effort';
SELECT toUnixTimestamp64Micro(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2286-11-20 17:46:40.123456"}', 'JSON(a DateTime64(6, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort', compatibility = '26.7';

SELECT 'remote, after 2286, fractional, Europe/Berlin, best_effort';
SELECT toUnixTimestamp64Micro(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2286-11-20 18:46:40.123456"}', 'JSON(a DateTime64(6, \'Europe/Berlin\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort', compatibility = '26.7';

SELECT 'remote, before 1970, whole seconds, best_effort';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1969-12-31 23:59:58.000"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort', compatibility = '26.7';

SELECT 'remote, in 1900, fractional, best_effort';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1900-01-01 00:00:00.5"}', 'JSON(a DateTime64(1, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort', compatibility = '26.7';

SELECT 'remote, after 2286, fractional, best_effort_us';
SELECT toUnixTimestamp64Micro(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2286-11-20 17:46:40.123456"}', 'JSON(a DateTime64(6, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort_us', compatibility = '26.7';

SELECT 'remote, after 2286, fractional, Europe/Berlin, best_effort_us';
SELECT toUnixTimestamp64Micro(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"2286-11-20 18:46:40.123456"}', 'JSON(a DateTime64(6, \'Europe/Berlin\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort_us', compatibility = '26.7';

SELECT 'remote, before 1970, whole seconds, best_effort_us';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1969-12-31 23:59:58.000"}', 'JSON(a DateTime64(3, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort_us', compatibility = '26.7';

SELECT 'remote, in 1900, fractional, best_effort_us';
SELECT toUnixTimestamp64Milli(json.a) AS v
FROM (SELECT materialize(CAST('{"a":"1900-01-01 00:00:00.5"}', 'JSON(a DateTime64(1, \'UTC\'))')) AS json FROM remote('127.0.0.1', system.one))
ORDER BY v
SETTINGS date_time_input_format = 'best_effort_us', compatibility = '26.7';
