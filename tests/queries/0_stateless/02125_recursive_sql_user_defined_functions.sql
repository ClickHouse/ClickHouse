-- Tags: no-parallel

DROP FUNCTION IF EXISTS 02125_function;
CREATE FUNCTION 02125_function AS x -> 02125_function(x);
SELECT 02125_function(1); --{serverError 1};
DROP FUNCTION 02125_function;

DROP FUNCTION IF EXISTS 02125_function_1;
CREATE FUNCTION 02125_function_1 AS x -> 02125_function_2(x);

DROP FUNCTION IF EXISTS 02125_function_2;
CREATE FUNCTION 02125_function_2 AS x -> 02125_function_1(x);

SELECT 02125_function_1(1); --{serverError 1};
SELECT 02125_function_2(2); --{serverError 1};

CREATE OR REPLACE FUNCTION 02125_function_2 AS x -> x + 1;

SELECT 02125_function_1(1);
SELECT 02125_function_2(2);

DROP FUNCTION 02125_function_1;
DROP FUNCTION 02125_function_2;
