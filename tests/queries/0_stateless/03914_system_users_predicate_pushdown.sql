-- Tags: no-fasttest, no-parallel
-- Verify that predicate pushdown on `system.users` by `name` column works correctly.
-- Fast path (O(1) lookups) is used for `name = 'literal'` and `name IN (...)` predicates.

DROP USER IF EXISTS test_pushdown_alice;
DROP USER IF EXISTS test_pushdown_bob;

CREATE USER test_pushdown_alice;
CREATE USER test_pushdown_bob;

-- Fast path: single name equality lookup (must read exactly 1 row)
SELECT name FROM system.users WHERE name = 'test_pushdown_alice' SETTINGS max_rows_to_read = 1;

-- Fast path: non-existent user returns empty result (reads 0 rows)
SELECT name FROM system.users WHERE name = 'test_pushdown_nonexistent' SETTINGS max_rows_to_read = 0;

-- Fast path: IN predicate (must read exactly 2 rows)
SELECT name FROM system.users WHERE name IN ('test_pushdown_alice', 'test_pushdown_bob') ORDER BY name SETTINGS max_rows_to_read = 2;

-- Fallback path: LIKE predicate still works (no row limit)
SELECT name FROM system.users WHERE name LIKE 'test_pushdown_%' ORDER BY name;

-- Fallback path: count all users still works (no predicate)
SELECT count() > 0 FROM system.users;

DROP USER test_pushdown_alice;
DROP USER test_pushdown_bob;
