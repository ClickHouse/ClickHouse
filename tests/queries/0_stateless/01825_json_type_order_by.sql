-- Tags: no-fasttest

SET enable_json_type = 1;
<<<<<<< HEAD:tests/queries/0_stateless/01825_new_type_json_order_by.sql
SET allow_not_comparable_types_in_order_by = 1;
=======
>>>>>>> origin/master:tests/queries/0_stateless/01825_json_type_order_by.sql
SELECT dummy FROM system.one ORDER BY materialize('{"k":"v"}'::JSON);
SELECT dummy FROM system.one ORDER BY materialize('{"k":"v"}'::JSON), dummy;
SELECT materialize('{"k":"v"}'::JSON) SETTINGS extremes = 1;
