-- Tags: no-replicated-database
-- ^ `CREATE CLONE AS is not supported with Replicated databases`

-- `CLONE AS src` copies the constraints of another table into a brand-new definition, the same way
-- `CREATE TABLE ... AS src` does (see `05069_reject_array_join_in_check_constraint.sql`, which screens
-- both for an `arrayJoin`). The copied constraint keeps being enforced.

DROP TABLE IF EXISTS t_clone_as_constraints;
DROP TABLE IF EXISTS t_clone_as_constraints_clone;

CREATE TABLE t_clone_as_constraints (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK length(arr) > 0) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_clone_as_constraints VALUES (1, [1, 2]);

CREATE TABLE t_clone_as_constraints_clone CLONE AS t_clone_as_constraints;
INSERT INTO t_clone_as_constraints_clone VALUES (2, []); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_clone_as_constraints_clone VALUES (2, [3]);
SELECT count() FROM t_clone_as_constraints_clone;

DROP TABLE t_clone_as_constraints;
DROP TABLE t_clone_as_constraints_clone;
