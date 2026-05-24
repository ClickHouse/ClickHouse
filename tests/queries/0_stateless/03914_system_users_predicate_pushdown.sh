#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that predicate pushdown on system.users by name column works correctly.
# Fast path (O(1) lookups) is used for name = 'literal' and name IN (...) predicates.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
CREATE USER test_pushdown_alice;
CREATE USER test_pushdown_bob;

-- Fast path: single name equality lookup
SELECT name FROM system.users WHERE name = 'test_pushdown_alice' SETTINGS max_rows_to_read = 1;

-- Fast path: non-existent user returns empty result
SELECT name FROM system.users WHERE name = 'test_pushdown_nonexistent' SETTINGS max_rows_to_read = 0;

-- Fast path: IN predicate
SELECT name FROM system.users WHERE name IN ('test_pushdown_alice', 'test_pushdown_bob') ORDER BY name SETTINGS max_rows_to_read = 2;

-- Fast path: contradictory AND uses intersection semantics and emits no rows
-- (with union semantics this would read 2 rows from the source and exceed max_rows_to_read = 1)
SELECT name FROM system.users WHERE name = 'test_pushdown_alice' AND name = 'test_pushdown_bob' SETTINGS max_rows_to_read = 1;

-- Fast path: AND of equality and IN narrows candidates via intersection
SELECT name FROM system.users WHERE name = 'test_pushdown_alice' AND name IN ('test_pushdown_alice', 'test_pushdown_bob') SETTINGS max_rows_to_read = 1;

-- Fast path: equality combined with an unrelated condition still narrows by name
SELECT name FROM system.users WHERE name = 'test_pushdown_alice' AND default_database = '' SETTINGS max_rows_to_read = 1;

-- Fallback path: LIKE predicate still works
SELECT name FROM system.users WHERE name LIKE 'test_pushdown_%' ORDER BY name;

-- Fallback path: count all users still works (no predicate)
SELECT count() > 0 FROM system.users;
"
