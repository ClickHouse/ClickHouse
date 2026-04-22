-- Regression for https://github.com/ClickHouse/ClickHouse/issues/101357
-- GRANT role TO itself must be rejected with BAD_ARGUMENTS (code 36).

DROP ROLE IF EXISTS test_role_04109_a;
DROP ROLE IF EXISTS test_role_04109_b;
CREATE ROLE test_role_04109_a;
CREATE ROLE test_role_04109_b;

-- Direct self-grant.
GRANT test_role_04109_a TO test_role_04109_a; -- { serverError BAD_ARGUMENTS }

-- Multi-role grant with one self-grant must still be rejected.
GRANT test_role_04109_a, test_role_04109_b TO test_role_04109_a; -- { serverError BAD_ARGUMENTS }

-- Legitimate role-to-role grant still works.
GRANT test_role_04109_a TO test_role_04109_b;

-- Nothing has been granted to test_role_04109_a as a consequence.
SHOW GRANTS FOR test_role_04109_a;

DROP ROLE test_role_04109_a;
DROP ROLE test_role_04109_b;
