-- Tags: no-fasttest, no-parallel
-- Verify that predicate pushdown on `system.users` by `name` column works correctly,
-- and that the fast path (O(1) lookup) is used for simple `name = 'literal'` predicates.

DROP USER IF EXISTS test_pushdown_alice;
DROP USER IF EXISTS test_pushdown_bob;

CREATE USER test_pushdown_alice;
CREATE USER test_pushdown_bob;

-- Fast path: single name equality lookup
SELECT name FROM system.users WHERE name = 'test_pushdown_alice';

-- Fast path: non-existent user returns empty result
SELECT name FROM system.users WHERE name = 'test_pushdown_nonexistent';

-- Fallback path: other predicates still work
SELECT name FROM system.users WHERE name IN ('test_pushdown_alice', 'test_pushdown_bob') ORDER BY name;
SELECT name FROM system.users WHERE name LIKE 'test_pushdown_%' ORDER BY name;

-- Fallback path: count all users still works (no predicate)
SELECT count() > 0 FROM system.users;

-- Verify the fast path reads exactly 1 row via query_log (requires query_log to be enabled)
SYSTEM FLUSH LOGS;

SELECT argMax(read_rows, event_time_microseconds)
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%SELECT name FROM system.users WHERE name = \'test_pushdown_alice\'%'
  AND type = 'QueryFinish';

-- Verify the fallback path reads more rows than the fast path
SELECT argMax(read_rows, event_time_microseconds) >= 2
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE '%SELECT name FROM system.users WHERE name LIKE \'test\_pushdown\_%\'%'
  AND type = 'QueryFinish';

DROP USER test_pushdown_alice;
DROP USER test_pushdown_bob;
