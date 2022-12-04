-- Tags: no-parallel

CREATE FUNCTION IF NOT EXISTS _02102_test_function AS x -> x + 1;
SELECT _02102_test_function(1);

CREATE FUNCTION _02102_test_function AS x -> x + 1; --{serverError 609}
CREATE FUNCTION IF NOT EXISTS _02102_test_function AS x -> x + 1;
DROP FUNCTION _02102_test_function;
