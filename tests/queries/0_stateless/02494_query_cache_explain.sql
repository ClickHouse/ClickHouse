-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_analyzer = 1;
SET allow_experimental_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- Run a silly query with a non-trivial plan and put the result into the query cache QC
SELECT 1 + number from system.numbers LIMIT 1 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

-- EXPLAIN PLAN should show the same regardless if the result is calculated or read from the QC
EXPLAIN PLAN SELECT 1 + number from system.numbers LIMIT 1;
EXPLAIN PLAN SELECT 1 + number from system.numbers LIMIT 1 SETTINGS use_query_cache = true; -- (*)

-- EXPLAIN PIPELINE should show the same regardless if the result is calculated or read from the QC
EXPLAIN PIPELINE SELECT 1 + number from system.numbers LIMIT 1;
EXPLAIN PIPELINE SELECT 1 + number from system.numbers LIMIT 1 SETTINGS use_query_cache = true; -- (*)

-- Statements (*) must not cache their results into the QC
SELECT count(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
