-- Tags: no-parallel

DROP FUNCTION IF EXISTS _02125_function;
CREATE FUNCTION _02125_function AS x -> _02125_function(x);
SELECT _02125_function(1); --{serverError 1};
DROP FUNCTION _02125_function;

DROP FUNCTION IF EXISTS _02125_function_1;
CREATE FUNCTION _02125_function_1 AS x -> _02125_function_2(x);

DROP FUNCTION IF EXISTS _02125_function_2;
CREATE FUNCTION _02125_function_2 AS x -> _02125_function_1(x);

SELECT _02125_function_1(1); --{serverError 1};
SELECT _02125_function_2(2); --{serverError 1};

CREATE OR REPLACE FUNCTION _02125_function_2 AS x -> x + 1;

SELECT _02125_function_1(1);
SELECT _02125_function_2(2);

DROP FUNCTION _02125_function_1;
DROP FUNCTION _02125_function_2;
