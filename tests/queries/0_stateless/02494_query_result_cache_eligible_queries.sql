-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE IF EXISTS eligible_test;
DROP TABLE IF EXISTS eligible_test2;

-- enable query result cache session-wide but also force it individually in each of below statements
SET enable_experimental_query_result_cache = true;

-- check that SELECT statements create entries in the query result cache ...
SELECT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- ... yet EXPLAIN SELECT should not create such an entry ...
EXPLAIN SELECT 2 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- ... and all other statements also should not create entries:

-- CREATE
CREATE TABLE eligible_test (a String) ENGINE=MergeTree ORDER BY a; --  SETTINGS enable_experimental_query_result_cache = true; -- SETTINGS rejected as unknown
SELECT count(*) FROM system.queryresult_cache;

-- ALTER
ALTER TABLE eligible_test ADD COLUMN b String SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- INSERT
INSERT INTO eligible_test VALUES('a', 'b'); -- SETTINGS enable_experimental_query_result_cache = true; -- SETTINGS rejected as unknown
SELECT count(*) FROM system.queryresult_cache;
INSERT INTO eligible_test SELECT * FROM eligible_test SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- SHOW
SHOW TABLES SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- CHECK
CHECK TABLE eligible_test SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- DESCRIBE
DESCRIBE TABLE eligible_test SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- EXISTS
EXISTS TABLE eligible_test SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- KILL
KILL QUERY WHERE query_id='3-857d-4a57-9ee0-3c7da5d60a90' SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- OPTIMIZE
OPTIMIZE TABLE eligible_test FINAL SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- TRUNCATE
TRUNCATE TABLE eligible_test SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

-- RENAME
RENAME TABLE eligible_test TO eligible_test2 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
DROP TABLE eligible_test2;

-- { echoOff }
