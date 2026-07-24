-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;
DROP TABLE IF EXISTS eligible_test;
DROP TABLE IF EXISTS eligible_test2;

-- enable query cache session-wide but also force it individually in each of below statements
SET use_query_cache = true;
SET query_cache_system_table_handling = 'save';

-- check that SELECT statements create entries in the query cache ...
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;

-- ... and all other statements also should not create entries:

-- CREATE
CREATE TABLE eligible_test (a String) ENGINE=MergeTree ORDER BY a; --  SETTINGS use_query_cache = true; -- SETTINGS rejected as unknown
SELECT COUNT(*) FROM system.query_cache;

-- ALTER
ALTER TABLE eligible_test ADD COLUMN b String SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- INSERT
INSERT INTO eligible_test VALUES('a', 'b'); -- SETTINGS use_query_cache = true; -- SETTINGS rejected as unknown
SELECT COUNT(*) FROM system.query_cache;
INSERT INTO eligible_test SELECT * FROM eligible_test SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- SHOW
SHOW TABLES SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- CHECK
CHECK TABLE eligible_test SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- DESCRIBE
DESCRIBE TABLE eligible_test SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- EXISTS
EXISTS TABLE eligible_test SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- KILL
KILL QUERY WHERE query_id='3-857d-4a57-9ee0-3c7da5d60a90' SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- OPTIMIZE
OPTIMIZE TABLE eligible_test FINAL SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- TRUNCATE
TRUNCATE TABLE eligible_test SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

-- RENAME
RENAME TABLE eligible_test TO eligible_test2 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
DROP TABLE eligible_test2;
