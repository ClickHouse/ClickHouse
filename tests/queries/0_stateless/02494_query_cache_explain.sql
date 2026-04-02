-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;
SET query_cache_system_table_handling = 'save';

SYSTEM CLEAR QUERY CACHE;

-- Run a silly query with a non-trivial plan and put the result into the query cache QC
SELECT 1 + number from system.numbers LIMIT 1 SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache;

-- EXPLAIN PLAN should show the same regardless if the result is calculated or read from the QC
EXPLAIN PLAN SELECT 1 + number from system.numbers LIMIT 1 SETTINGS query_plan_merge_expressions = 1, query_plan_execute_functions_after_sorting = 0, query_plan_push_down_limit = 1;
EXPLAIN PLAN SELECT 1 + number from system.numbers LIMIT 1 SETTINGS use_query_cache = true, query_plan_merge_expressions = 1, query_plan_execute_functions_after_sorting = 0; -- (*)

-- EXPLAIN PIPELINE should show the same regardless if the result is calculated or read from the QC
EXPLAIN PIPELINE SELECT 1 + number from system.numbers LIMIT 1 SETTINGS query_plan_merge_expressions = 1, query_plan_execute_functions_after_sorting = 0, query_plan_push_down_limit = 1;
EXPLAIN PIPELINE SELECT 1 + number from system.numbers LIMIT 1 SETTINGS use_query_cache = true, query_plan_merge_expressions = 1, query_plan_execute_functions_after_sorting = 0; -- (*)

-- Statements (*) must not cache their results into the QC
SELECT count(*) FROM system.query_cache;

SYSTEM CLEAR QUERY CACHE;
