-- Tags: no-parallel

DROP FUNCTION IF EXISTS _02148_test_function;
CREATE FUNCTION _02148_test_function AS () -> (SELECT 1);

SELECT _02148_test_function();

CREATE OR REPLACE FUNCTION _02148_test_function AS () -> (SELECT 2);

SELECT _02148_test_function();

DROP FUNCTION _02148_test_function;

CREATE FUNCTION _02148_test_function AS (x) -> (SELECT x + 1);
SELECT _02148_test_function(1);

DROP FUNCTION IF EXISTS _02148_test_function_nested;
CREATE FUNCTION _02148_test_function_nested AS (x) -> _02148_test_function(x + 2);
SELECT _02148_test_function_nested(1);

DROP FUNCTION _02148_test_function;
DROP FUNCTION _02148_test_function_nested;

DROP TABLE IF EXISTS _02148_test_table;
CREATE TABLE _02148_test_table (id UInt64, value String) ENGINE=TinyLog;
INSERT INTO _02148_test_table VALUES (0, 'Value');

CREATE FUNCTION _02148_test_function AS () -> (SELECT * FROM _02148_test_table LIMIT 1);
SELECT _02148_test_function();

CREATE OR REPLACE FUNCTION _02148_test_function AS () -> (SELECT value FROM _02148_test_table LIMIT 1);
SELECT _02148_test_function();

DROP FUNCTION _02148_test_function;
DROP TABLE _02148_test_table;
