-- Tags: no-parallel
-- Tag no-parallel: `SYSTEM SYNC FILE CACHE` performs a host-wide sync() syscall that flushes all
-- dirty filesystem buffers, and `SYSTEM RELOAD FUNCTIONS` is process-wide; running them alongside
-- other tests stalls the concurrent tests unboundedly and risks timeouts, so it must run alone.
-- Coverage for uncovered branches in InterpreterSystemQuery.cpp:
-- SYNC FILE CACHE, RELOAD FUNCTIONS.

SYSTEM SYNC FILE CACHE;

SYSTEM RELOAD FUNCTIONS;

-- RELOAD EMBEDDED DICTIONARIES requires regions_hierarchy.txt which is only
-- present in CI config; skip locally.
-- SYNC TRANSACTION LOG is only valid when transactions are enabled; skip.
-- RELOAD FUNCTION requires an existing user-defined function; skip.

SELECT 'done';
