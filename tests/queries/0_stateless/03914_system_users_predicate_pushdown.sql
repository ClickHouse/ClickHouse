-- Tags: no-fasttest, no-parallel
-- Verify that predicate pushdown on `system.users` by `name` column works correctly.
-- Fast path (O(1) lookups) is used for `name = 'literal'` and `name IN (...)` predicates.

DROP USER IF EXISTS test_pushdown_alice;
DROP USER IF EXISTS test_pushdown_bob;

CREATE USER test_pushdown_alice;
CREATE USER test_pushdown_bob;

-- Fast path: single name equality lookup
SELECT name FROM system.users WHERE name = 'test_pushdown_alice';

-- Fast path: non-existent user returns empty result
SELECT name FROM system.users WHERE name = 'test_pushdown_nonexistent';

-- Fast path: IN predicate
SELECT name FROM system.users WHERE name IN ('test_pushdown_alice', 'test_pushdown_bob') ORDER BY name;

-- Fallback path: LIKE predicate still works
SELECT name FROM system.users WHERE name LIKE 'test_pushdown_%' ORDER BY name;

-- Fallback path: count all users still works (no predicate)
SELECT count() > 0 FROM system.users;

-- Verify the fast path reads exactly 1 row via query_log
SYSTEM FLUSH LOGS query_log;

SELECT argMax(read_rows, event_time_microseconds)
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - 600
  AND query LIKE '%SELECT name FROM system.users WHERE name = \'test_pushdown_alice\'%'
  AND type = 'QueryFinish';

-- Verify the IN fast path reads exactly 2 rows
SELECT argMax(read_rows, event_time_microseconds)
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - 600
  AND query LIKE '%SELECT name FROM system.users WHERE name IN (\'test_pushdown_alice\', \'test_pushdown_bob\')%'
  AND type = 'QueryFinish';

-- Verify the fallback path reads more rows than the fast path
SELECT argMax(read_rows, event_time_microseconds) >= 2
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - 600
  AND query LIKE '%SELECT name FROM system.users WHERE name LIKE \'test\_pushdown\_%\'%'
  AND type = 'QueryFinish';

DROP USER test_pushdown_alice;
DROP USER test_pushdown_bob;
