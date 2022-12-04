-- Tags: no-parallel

DROP FUNCTION IF EXISTS _02126_function;
CREATE FUNCTION _02126_function AS x -> x;
SELECT _02126_function(1);
DROP FUNCTION _02126_function;

CREATE FUNCTION _02126_function AS () -> x;
SELECT _02126_function(); --{ serverError 47 }
DROP FUNCTION _02126_function;

CREATE FUNCTION _02126_function AS () -> 5;
SELECT _02126_function();
DROP FUNCTION _02126_function;
