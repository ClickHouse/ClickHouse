-- Tags: no-parallel

DROP FUNCTION IF EXISTS 02148_test_function;
CREATE FUNCTION 02148_test_function AS () -> (SELECT 1);

SELECT 02148_test_function();

CREATE OR REPLACE FUNCTION 02148_test_function AS () -> (SELECT 2);

SELECT 02148_test_function();

DROP FUNCTION 02148_test_function;

CREATE FUNCTION 02148_test_function AS (x) -> (SELECT x + 1);
SELECT 02148_test_function(1);

DROP FUNCTION IF EXISTS 02148_test_function_nested;
CREATE FUNCTION 02148_test_function_nested AS (x) -> 02148_test_function(x + 2);
SELECT 02148_test_function_nested(1);

DROP FUNCTION 02148_test_function;
DROP FUNCTION 02148_test_function_nested;

DROP TABLE IF EXISTS 02148_test_table;
CREATE TABLE 02148_test_table (id UInt64, value String) ENGINE=TinyLog;
INSERT INTO 02148_test_table VALUES (0, 'Value');

CREATE FUNCTION 02148_test_function AS () -> (SELECT * FROM 02148_test_table LIMIT 1);
SELECT 02148_test_function();

CREATE OR REPLACE FUNCTION 02148_test_function AS () -> (SELECT value FROM 02148_test_table LIMIT 1);
SELECT 02148_test_function();

DROP FUNCTION 02148_test_function;
DROP TABLE 02148_test_table;
