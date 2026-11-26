-- `arrayJoin` is rejected in a constraint expression (see
-- `05069_reject_array_join_in_check_constraint.sql`) wherever the expression is stated, including by an
-- `ALTER` command that would go on to install nothing: an `ADD CONSTRAINT IF NOT EXISTS` of a name that
-- is taken, and a `MODIFY CONSTRAINT IF EXISTS` of a name that is not there. A constraint name declared
-- more than once is rejected as well, so no table is left holding a declaration whose name is ambiguous.

DROP TABLE IF EXISTS t_constraint_repeated_name;
DROP TABLE IF EXISTS t_alter_constraint_array_join;

-- `DROP CONSTRAINT` erases and `MODIFY CONSTRAINT` replaces the first declaration of a name, so a second
-- declaration of it is reachable only once the first one has been dropped (see
-- `05234_constraint_repeated_name_legacy_metadata.sh`).
CREATE TABLE t_constraint_repeated_name (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK k > 0, CONSTRAINT c CHECK k < 1000)
ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_constraint_repeated_name (k UInt32, CONSTRAINT c CHECK k > 0, CONSTRAINT c ASSUME k < 1000)
ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

-- A full-definition `ATTACH` states the definition in the query itself, so it is user input like
-- `CREATE` is, rather than a replay of metadata this server has already accepted.
ATTACH TABLE t_constraint_repeated_name UUID 'c2d4f1b7-3a86-4e52-9f0c-71d5e8a29b64'
(k UInt32, CONSTRAINT c CHECK k > 0, CONSTRAINT c CHECK k < 1000)
ENGINE = MergeTree ORDER BY k; -- { serverError INCORRECT_QUERY }

CREATE TABLE t_constraint_repeated_name (k UInt32, CONSTRAINT c CHECK k > 0, CONSTRAINT c2 CHECK k < 1000)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_constraint_repeated_name VALUES (0); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_constraint_repeated_name VALUES (1);
SELECT count() FROM t_constraint_repeated_name;

CREATE TABLE t_alter_constraint_array_join (k UInt32, arr Array(UInt32), CONSTRAINT c CHECK length(arr) > 0)
ENGINE = MergeTree ORDER BY k;

-- The name is taken, so `apply()` would install nothing.
ALTER TABLE t_alter_constraint_array_join ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
-- There is no such name, so `apply()` would install nothing.
ALTER TABLE t_alter_constraint_array_join MODIFY CONSTRAINT IF EXISTS absent CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
-- Without `IF EXISTS` the expression is reported ahead of the missing name that `apply()` would report.
ALTER TABLE t_alter_constraint_array_join MODIFY CONSTRAINT absent CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
-- The name is free, so the declaration would be installed.
ALTER TABLE t_alter_constraint_array_join ADD CONSTRAINT IF NOT EXISTS c2 CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }

-- The whole `ALTER` is refused whichever of its commands states the expression, and whatever the
-- commands around it do to the name.
ALTER TABLE t_alter_constraint_array_join ADD CONSTRAINT IF NOT EXISTS d CHECK k < 1000, MODIFY CONSTRAINT IF EXISTS d CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_alter_constraint_array_join DROP CONSTRAINT c, ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0; -- { serverError INCORRECT_QUERY }
ALTER TABLE t_alter_constraint_array_join ADD CONSTRAINT IF NOT EXISTS c CHECK arrayJoin(arr) > 0, DROP CONSTRAINT c; -- { serverError INCORRECT_QUERY }

-- Every statement above left the table alone: `c` still refuses an empty array, and a row that satisfies
-- it is accepted - which a stored `arrayJoin` constraint would have made impossible, since a block whose
-- element count differs from its row count is refused outright.
INSERT INTO t_alter_constraint_array_join VALUES (1, []); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO t_alter_constraint_array_join VALUES (1, [1, 2]);
SELECT count() FROM t_alter_constraint_array_join;
SELECT create_table_query LIKE '%arrayJoin%' FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_constraint_array_join';

DROP TABLE t_alter_constraint_array_join;
DROP TABLE t_constraint_repeated_name;
