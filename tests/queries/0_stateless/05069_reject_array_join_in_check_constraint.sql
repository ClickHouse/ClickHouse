-- A `CHECK` constraint is evaluated per block and read by block row, so an `arrayJoin` inside it checks a
-- row against another row's value, or - when an array is empty and the column ends up shorter than the
-- block - past the end of it. It is rejected at DDL time, like it already is for keys and indexes.

DROP TABLE IF EXISTS t_check_array_join;
DROP TABLE IF EXISTS t_check_array_join_subquery;

CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK unnest(arr) > 0) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }
CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK UNNEST(arr) > 0) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

-- An `ASSUME` constraint is substituted into queries by the constraint optimizer, and it has no more
-- business changing the number of rows than a `CHECK` has.
CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c ASSUME arrayJoin(arr) > 0) ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_check_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK length(arr) > 0) ENGINE = MergeTree ORDER BY k;
SELECT 'accepted';

ALTER TABLE t_check_array_join ADD CONSTRAINT c2 CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_check_array_join ADD CONSTRAINT c2 CHECK k < 1000;
-- `MODIFY CONSTRAINT` replaces the stored declaration in place, so it states a new expression too.
ALTER TABLE t_check_array_join MODIFY CONSTRAINT c2 CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
SELECT 'altered';

INSERT INTO t_check_array_join VALUES (1, [1, 2]);
SELECT count() FROM t_check_array_join;

-- An `arrayJoin` inside a subquery has a scope of its own: it multiplies the rows of that subquery, and
-- what reaches the constraint is the set it returns. Such a constraint is accepted and enforced.
CREATE TABLE t_check_array_join_subquery (k UInt32, CONSTRAINT c CHECK k IN (SELECT arrayJoin([1, 2, 3]))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_check_array_join_subquery VALUES (2);
INSERT INTO t_check_array_join_subquery VALUES (4); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'subquery', count() FROM t_check_array_join_subquery;

DROP TABLE t_check_array_join;
DROP TABLE t_check_array_join_subquery;
