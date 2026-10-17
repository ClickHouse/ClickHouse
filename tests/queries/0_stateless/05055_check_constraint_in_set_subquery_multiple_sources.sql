-- Tags: no-async-insert
-- - no-async-insert -- the `IN (SELECT ...)` constraint is evaluated in the background flush, whose context is created from the global context and therefore has `default` as the current database, so the unqualified table name cannot be resolved. This is a pre-existing property of the asynchronous insert queue, unrelated to the constraint machinery.

-- The set subquery of a `CHECK` constraint is planned on its own, detached from the expression
-- it came from. It therefore needs its own unique `__tableN` table aliases: without them the
-- planner derives a column identifier from the bare column name, and a subquery reading the same
-- column name from two sources registers that identifier twice.

DROP TABLE IF EXISTS check_in_set_multi_src;
DROP TABLE IF EXISTS check_in_set_multi;

CREATE TABLE check_in_set_multi_src (id Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_in_set_multi_src VALUES (1);

-- A nested subquery: the outer query and the inner query both expose a column named `id`.
CREATE TABLE check_in_set_multi (c0 Int, CONSTRAINT c0 CHECK c0 IN (SELECT id FROM (SELECT id FROM check_in_set_multi_src)))
ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_in_set_multi (c0) VALUES (1);
SELECT count() FROM check_in_set_multi;
DROP TABLE check_in_set_multi;

-- A self-join: both sides expose a column named `id`.
CREATE TABLE check_in_set_multi (c0 Int, CONSTRAINT c0 CHECK c0 IN (SELECT l.id FROM check_in_set_multi_src AS l, check_in_set_multi_src AS r))
ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO check_in_set_multi (c0) VALUES (1);
SELECT count() FROM check_in_set_multi;
DROP TABLE check_in_set_multi;

DROP TABLE check_in_set_multi_src;
