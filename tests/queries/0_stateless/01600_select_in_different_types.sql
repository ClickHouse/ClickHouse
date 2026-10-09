SELECT 1 IN (SELECT 1);
SELECT -1 IN (SELECT 1);

DROP TABLE IF EXISTS select_in_test;

CREATE TABLE select_in_test(value UInt8) ENGINE=TinyLog;
INSERT INTO select_in_test VALUES (1), (2), (3);

SELECT value FROM select_in_test WHERE value IN (-1);
SELECT value FROM select_in_test WHERE value IN (SELECT -1);

SELECT value FROM select_in_test WHERE value IN (1);
SELECT value FROM select_in_test WHERE value IN (SELECT 1);

DROP TABLE select_in_test;

CREATE TABLE select_in_test(value Int8) ENGINE=TinyLog;
INSERT INTO select_in_test VALUES (-1), (2), (3);

SELECT value FROM select_in_test WHERE value IN (1);
SELECT value FROM select_in_test WHERE value IN (SELECT 1);

SELECT value FROM select_in_test WHERE value IN (2);
SELECT value FROM select_in_test WHERE value IN (SELECT 2);

DROP TABLE select_in_test;

SELECT 1 IN (1);
SELECT '1' IN (SELECT 1);

SELECT 1 IN (SELECT 1) SETTINGS transform_null_in = 1;
SELECT 1 IN (SELECT 'a') SETTINGS transform_null_in = 1;
SELECT 'a' IN (SELECT 1) SETTINGS transform_null_in = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT 1 IN (SELECT -1) SETTINGS transform_null_in = 1;
SELECT -1 IN (SELECT 1) SETTINGS transform_null_in = 1; -- { serverError CANNOT_CONVERT_TYPE }
