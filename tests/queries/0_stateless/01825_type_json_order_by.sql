-- Tags: no-fasttest

SET allow_experimental_object_type = 1;
SET allow_not_comparable_types_in_order_by = 1;
SELECT dummy FROM system.one ORDER BY materialize('{"k":"v"}'::Object('json'));
SELECT dummy FROM system.one ORDER BY materialize('{"k":"v"}'::Object('json')), dummy;
SELECT materialize('{"k":"v"}'::Object('json')) SETTINGS extremes = 1;
