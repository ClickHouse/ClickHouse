-- Tags: no-parallel

DROP FUNCTION IF EXISTS 02126_function;
CREATE FUNCTION 02126_function AS x -> x;
SELECT 02126_function(1);
DROP FUNCTION 02126_function;

CREATE FUNCTION 02126_function AS () -> x;
SELECT 02126_function(); --{ serverError UNKNOWN_IDENTIFIER }
DROP FUNCTION 02126_function;

CREATE FUNCTION 02126_function AS () -> 5;
SELECT 02126_function();
DROP FUNCTION 02126_function;
