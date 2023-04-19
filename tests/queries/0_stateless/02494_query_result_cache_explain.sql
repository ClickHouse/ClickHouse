-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY RESULT CACHE;

-- Run a silly query with a non-trivial plan and put the result into the query result cache (QRC)
SELECT 1 + number from system.numbers LIMIT 1 SETTINGS enable_experimental_query_result_cache = true;
SELECT count(*) FROM system.query_result_cache;

-- EXPLAIN PLAN should show the same regardless if the result is calculated or read from the QRC
EXPLAIN PLAN SELECT 1 + number from system.numbers LIMIT 1;
EXPLAIN PLAN SELECT 1 + number from system.numbers LIMIT 1 SETTINGS enable_experimental_query_result_cache = true; -- (*)

-- EXPLAIN PIPELINE should show the same regardless if the result is calculated or read from the QRC
EXPLAIN PIPELINE SELECT 1 + number from system.numbers LIMIT 1;
EXPLAIN PIPELINE SELECT 1 + number from system.numbers LIMIT 1 SETTINGS enable_experimental_query_result_cache = true; -- (*)

-- Statements (*) must not cache their results into the QRC
SELECT count(*) FROM system.query_result_cache;

SYSTEM DROP QUERY RESULT CACHE;
