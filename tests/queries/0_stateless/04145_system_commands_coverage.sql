-- Tags: no-parallel
-- no-parallel: SYSTEM SYNC FILE CACHE performs a host-wide sync() syscall that
-- flushes all dirty filesystem buffers; running it alongside other tests slows
-- it down unboundedly and stalls the concurrent tests, risking timeouts.
-- Coverage for uncovered branches in InterpreterSystemQuery.cpp:
-- SYNC FILE CACHE, RELOAD FUNCTIONS.

SYSTEM SYNC FILE CACHE;

SYSTEM RELOAD FUNCTIONS;

-- RELOAD EMBEDDED DICTIONARIES requires regions_hierarchy.txt which is only
-- present in CI config; skip locally.
-- SYNC TRANSACTION LOG is only valid when transactions are enabled; skip.
-- RELOAD FUNCTION requires an existing user-defined function; skip.

SELECT 'done';
