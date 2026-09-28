-- `arrayJoin` is rejected in a `CHECK` constraint (see
-- `05069_reject_array_join_in_check_constraint.sql`), but only in a command that really installs a
-- declaration. `ADD CONSTRAINT IF NOT EXISTS` of a name that is taken and `MODIFY CONSTRAINT IF EXISTS`
-- of a name that is not there install nothing, and keep being the no-ops they were before that check
-- existed.

DROP TABLE IF EXISTS t_alter_constraint_no_op;

CREATE TABLE t_alter_constraint_no_op (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK length(arr) > 0)
ENGINE = MergeTree ORDER BY k;

-- The name is taken, so nothing is installed.
ALTER TABLE t_alter_constraint_no_op ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0;
-- There is no such name, so nothing is installed.
ALTER TABLE t_alter_constraint_no_op MODIFY CONSTRAINT IF EXISTS absent CHECK arrayJoin(arr) > 0;
-- Without `IF EXISTS`, a missing name is reported as such, not as an `arrayJoin`.
ALTER TABLE t_alter_constraint_no_op MODIFY CONSTRAINT absent CHECK arrayJoin(arr) > 0; -- { serverError BAD_ARGUMENTS }

-- The name is free, so the declaration is installed - and rejected.
ALTER TABLE t_alter_constraint_no_op ADD CONSTRAINT IF NOT EXISTS c2 CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }

-- The names of this same `ALTER` are followed: `c` is gone by the time the `ADD` runs, so the
-- declaration is installed - and rejected. The reverse order leaves the name taken, so it is a no-op.
ALTER TABLE t_alter_constraint_no_op DROP CONSTRAINT c, ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_alter_constraint_no_op ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0, DROP CONSTRAINT c;

-- The last statement did drop `c`, and the ones before it changed nothing.
SELECT create_table_query LIKE '%CONSTRAINT%' FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_constraint_no_op';

DROP TABLE t_alter_constraint_no_op;
