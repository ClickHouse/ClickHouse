-- Tags: no-fasttest
-- Test for https://github.com/ClickHouse/ClickHouse/issues/102511
-- CASE with Dynamic/Variant expression should not use the transform path
-- because transform's hash-based lookup includes the type discriminator,
-- causing values with different stored subtypes to never match.

SET allow_experimental_dynamic_type = 1;

-- Dynamic type: CASE should match values correctly
DROP TABLE IF EXISTS t_case_dyn;
CREATE TABLE t_case_dyn (c0 Dynamic) ENGINE = Memory;
INSERT INTO t_case_dyn VALUES (1), (2), (3);

SELECT 'Dynamic CASE:';
SELECT c0, CASE c0 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS result
FROM t_case_dyn ORDER BY toString(c0);

-- Dynamic type with strings
DROP TABLE IF EXISTS t_case_dyn_str;
CREATE TABLE t_case_dyn_str (c0 Dynamic) ENGINE = Memory;
INSERT INTO t_case_dyn_str VALUES ('a'), ('b'), ('c');

SELECT 'Dynamic CASE with strings:';
SELECT c0, CASE c0 WHEN 'a' THEN 'first' WHEN 'b' THEN 'second' ELSE 'other' END AS result
FROM t_case_dyn_str ORDER BY toString(c0);

-- Dynamic type with NULLs
DROP TABLE IF EXISTS t_case_dyn_null;
CREATE TABLE t_case_dyn_null (c0 Dynamic) ENGINE = Memory;
INSERT INTO t_case_dyn_null VALUES (1), (NULL), (3);

SELECT 'Dynamic CASE with NULL:';
SELECT c0, CASE c0 WHEN 1 THEN 'one' WHEN NULL THEN 'null_val' ELSE 'other' END AS result
FROM t_case_dyn_null ORDER BY toString(c0);

DROP TABLE t_case_dyn;
DROP TABLE t_case_dyn_str;
DROP TABLE t_case_dyn_null;
