-- Tags: no-parallel:misc-caches
-- Tag no-parallel: issues `SYSTEM CLEAR DNS CACHE`, which drops the process-global DNS cache used by concurrent tests
SELECT * FROM remote('127..2', 'a.'); -- { serverError SYNTAX_ERROR }

-- Clear cache to avoid future errors in the logs
SYSTEM CLEAR DNS CACHE
