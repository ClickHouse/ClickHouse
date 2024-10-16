-- Tags: no-fasttest

SET allow_experimental_json_type = 1;
SELECT dummy FROM system.one ORDER BY materialize('{"k":"v"}'::JSON);
SELECT dummy FROM system.one ORDER BY materialize('{"k":"v"}'::JSON), dummy;
SELECT materialize('{"k":"v"}'::JSON) SETTINGS extremes = 1;
