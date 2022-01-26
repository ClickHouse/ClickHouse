-- Tags: no-parallel

DROP FUNCTION IF EXISTS 02187_test_function;
CREATE FUNCTION 02187_test_function AS (x) -> x + 1;
SELECT 02187_test_function(1);

CREATE FUNCTION 02187_test_function AS (x) -> x + 2; --{serverError 609}
CREATE FUNCTION IF NOT EXISTS 02187_test_function AS (x) -> x + 2;
CREATE TEMPORARY FUNCTION 02187_test_function AS (x) -> x + 2; --{serverError 609}
CREATE TEMPORARY FUNCTION IF NOT EXISTS 02187_test_function AS (x) -> x + 2;

SELECT 02187_test_function(1);

DROP FUNCTION 02187_test_function;

CREATE TEMPORARY FUNCTION 02187_test_function AS (x) -> x + 2;

SELECT 02187_test_function(1);
SELECT name, create_query FROM system.functions WHERE name = '02187_test_function' AND origin = 'SQLUserDefined';

CREATE FUNCTION 02187_test_function AS (x) -> x + 3; --{serverError 609}
CREATE TEMPORARY FUNCTION 02187_test_function AS (x) -> x + 3; --{serverError 609}
CREATE FUNCTION IF NOT EXISTS 02187_test_function AS (x) -> x + 3;
CREATE TEMPORARY FUNCTION IF NOT EXISTS 02187_test_function AS (x) -> x + 3;

SELECT 02187_test_function(1);

CREATE OR REPLACE FUNCTION 02187_test_function AS (x) -> x + 3;
SELECT 02187_test_function(1);

DROP FUNCTION 02187_test_function_; --{serverError 610}
DROP FUNCTION IF EXISTS 02187_test_function_;
DROP FUNCTION 02187_test_function;
