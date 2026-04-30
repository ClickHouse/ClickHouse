-- Coverage for uncovered branches in InterpreterSystemQuery.cpp:
-- SYNC FILE CACHE, RELOAD FUNCTIONS.

SYSTEM SYNC FILE CACHE;

SYSTEM RELOAD FUNCTIONS;

-- RELOAD EMBEDDED DICTIONARIES requires regions_hierarchy.txt which is only
-- present in CI config; skip locally.
-- SYNC TRANSACTION LOG is only valid when transactions are enabled; skip.
-- RELOAD FUNCTION requires an existing user-defined function; skip.

SELECT 'done';
