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

DROP USER test_pushdown_alice;
DROP USER test_pushdown_bob;
