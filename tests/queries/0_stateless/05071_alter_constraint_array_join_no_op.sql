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

-- An `ADD` that really installs takes the name for the rest of the statement, so the `MODIFY` that
-- follows it installs a declaration and is screened.
ALTER TABLE t_alter_constraint_no_op ADD CONSTRAINT IF NOT EXISTS d CHECK k < 1000, MODIFY CONSTRAINT IF EXISTS d CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }

-- An `ADD` of a name that is taken installs nothing even without `IF NOT EXISTS`, because `apply()`
-- refuses the command, so it must not make the name look taken twice: the `DROP` leaves no `c`, the
-- `MODIFY ... IF EXISTS` installs nothing, and the duplicate name is what is reported.
ALTER TABLE t_alter_constraint_no_op ADD CONSTRAINT c CHECK k > 0, DROP CONSTRAINT c, MODIFY CONSTRAINT IF EXISTS c CHECK arrayJoin(arr) > 0; -- { serverError ILLEGAL_COLUMN }

-- The names of this same `ALTER` are followed: `c` is gone by the time the `ADD` runs, so the
-- declaration is installed - and rejected. The reverse order leaves the name taken, so it is a no-op.
ALTER TABLE t_alter_constraint_no_op DROP CONSTRAINT c, ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_alter_constraint_no_op ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0, DROP CONSTRAINT c;

-- The last statement did drop `c`, and the ones before it changed nothing.
SELECT create_table_query LIKE '%CONSTRAINT%' FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_constraint_no_op';

DROP TABLE t_alter_constraint_no_op;

DROP TABLE IF EXISTS t_alter_constraint_repeated_name;

-- A name can be declared more than once, and `DROP CONSTRAINT` removes one declaration of it, so the
-- name is still taken after the drop.
CREATE TABLE t_alter_constraint_repeated_name (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK k > 0, CONSTRAINT c CHECK k < 1000)
ENGINE = MergeTree ORDER BY k;

-- A `c` is left for the `MODIFY` to replace, so the declaration is installed - and rejected.
ALTER TABLE t_alter_constraint_repeated_name DROP CONSTRAINT c, MODIFY CONSTRAINT IF EXISTS c CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
-- A `c` is left, so the name is taken and the `ADD` installs nothing.
ALTER TABLE t_alter_constraint_repeated_name DROP CONSTRAINT c, ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0;

-- No `arrayJoin` was installed by either statement, and the `DROP` of the second one took effect:
-- `k = 0` passes the `c CHECK k < 1000` that is left and fails the `c CHECK k > 0` that was dropped.
SELECT create_table_query LIKE '%arrayJoin%' FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_constraint_repeated_name';
INSERT INTO t_alter_constraint_repeated_name VALUES (0, [1, 2]);
SELECT count() FROM t_alter_constraint_repeated_name;

-- A no-op `ADD ... IF NOT EXISTS` must not make the name look taken twice: the `DROP` then leaves no `c`
-- at all, so the `MODIFY ... IF EXISTS` installs nothing and the statement is accepted.
ALTER TABLE t_alter_constraint_repeated_name ADD CONSTRAINT IF NOT EXISTS c CHECK k < 1000, DROP CONSTRAINT c, MODIFY CONSTRAINT IF EXISTS c CHECK arrayJoin(arr) > 0;
SELECT create_table_query LIKE '%CONSTRAINT%' FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_constraint_repeated_name';

DROP TABLE t_alter_constraint_repeated_name;
