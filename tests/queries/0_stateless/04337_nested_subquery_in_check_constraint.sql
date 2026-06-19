-- Tags: no-async-insert
-- - no-async-insert -- with wait_for_async_insert=0 the INSERT is fire-and-forget, so the constraint error is raised in the background flush and never reaches the client, breaking the { serverError } assertion.

-- A bare subquery in a CHECK constraint is rejected, but so must be a scalar subquery
-- nested under a function: otherwise an arbitrary subquery would run on every insert.
-- Only the set side of an IN operator is allowed (built lazily as a "not-ready set").

DROP TABLE IF EXISTS check_nested_scalar;
CREATE TABLE check_nested_scalar (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE check_nested_scalar ADD CONSTRAINT c0 CHECK equals((SELECT 1), 1);
INSERT INTO TABLE check_nested_scalar (c0) VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE check_nested_scalar;

-- A subquery nested deeper under several functions is still rejected.
CREATE TABLE check_deep_scalar (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE check_deep_scalar ADD CONSTRAINT c0 CHECK c0 + (SELECT 1) > 0;
INSERT INTO TABLE check_deep_scalar (c0) VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE check_deep_scalar;

-- A scalar subquery on the left-hand side of IN is rejected too (only the set side is allowed).
CREATE TABLE check_in_lhs (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE check_in_lhs ADD CONSTRAINT c0 CHECK (SELECT 1) IN (1, 2, 3);
INSERT INTO TABLE check_in_lhs (c0) VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE check_in_lhs;

-- A scalar subquery hidden inside the IN set side (a list, not a direct subquery) is
-- rejected as well: it is not a not-ready set, so it would run on every insert.
CREATE TABLE check_in_rhs_list (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE check_in_rhs_list ADD CONSTRAINT c0 CHECK c0 IN (1, (SELECT 1), 3);
INSERT INTO TABLE check_in_rhs_list (c0) VALUES (1); -- { serverError BAD_ARGUMENTS }
DROP TABLE check_in_rhs_list;

-- An IN subquery (a "not-ready set") remains allowed: the set is built lazily at insert time.
DROP TABLE IF EXISTS check_in_set_src;
CREATE TABLE check_in_set_src (id Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_in_set_src VALUES (1);
CREATE TABLE check_in_set (c0 Int, CONSTRAINT c0 CHECK c0 IN (SELECT id FROM check_in_set_src)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_in_set (c0) VALUES (1);
SELECT count() FROM check_in_set;
DROP TABLE check_in_set;
DROP TABLE check_in_set_src;
