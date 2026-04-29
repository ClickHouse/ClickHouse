-- Coverage for uncovered branches in InterpreterSystemQuery.cpp:
-- SYNC FILE CACHE, RELOAD FUNCTIONS, RELOAD EMBEDDED DICTIONARIES,
-- SYNC TRANSACTION LOG.

SYSTEM SYNC FILE CACHE;

SYSTEM RELOAD FUNCTIONS;

SYSTEM RELOAD EMBEDDED DICTIONARIES;

-- SYNC TRANSACTION LOG is only valid when transactions are enabled; skip.
-- RELOAD FUNCTION requires an existing user-defined function; skip.

SELECT 'done';
