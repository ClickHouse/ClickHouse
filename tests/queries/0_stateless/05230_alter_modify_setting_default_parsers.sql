-- `MODIFY SETTING name = DEFAULT` is a reset written in the modify syntax. Every parser rewrites it
-- into a `RESET SETTING` command, so that the storage sees a single spelling of a reset.

SELECT '-- the SQL parser turns a pure reset into `RESET SETTING`';
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY SETTING y = DEFAULT');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY COLUMN c MODIFY SETTING y = DEFAULT');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY COLUMN IF EXISTS c MODIFY SETTING y = DEFAULT');

SELECT '-- a command which also modifies is split in two';
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY SETTING x = 1, y = DEFAULT');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY COLUMN c MODIFY SETTING x = 1, y = DEFAULT');
SELECT formatQuerySingleLine('ALTER TABLE t ADD COLUMN z UInt64, MODIFY SETTING x = 1, y = DEFAULT');

SELECT '-- the rewritten query parses back to itself';
SELECT formatQuerySingleLine(formatQuerySingleLine('ALTER TABLE t MODIFY SETTING x = 1, y = DEFAULT'));

-- A setting cannot be both modified and reset, or reset twice, in one command. Such a command is
-- left as written, so that `AlterCommand::parse` rejects it while resolving the aliases of a setting.
SELECT '-- a contradictory command is not rewritten';
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY SETTING x = 1, x = DEFAULT');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY SETTING x = DEFAULT, x = DEFAULT');
SELECT formatQuerySingleLine('ALTER TABLE t MODIFY COLUMN c MODIFY SETTING x = 1, x = DEFAULT');

SELECT '-- the JSON parser rewrites the same way';
SELECT formatQuerySingleLine(formatQueryFromJSON('{"type":"AlterQuery","table":"t","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_SETTING","settings_changes":{"type":"SetQuery","changes":[{"name":"x","value":{"field_type":"UInt64","value":1}}],"default_settings":["y"]}}]}}'));
SELECT formatQuerySingleLine(formatQueryFromJSON('{"type":"AlterQuery","table":"t","alter_object":"TABLE","command_list":{"type":"ExpressionList","children":[{"type":"AlterCommand","command_type":"MODIFY_SETTING","settings_changes":{"type":"SetQuery","changes":[{"name":"x","value":{"field_type":"UInt64","value":1}}],"default_settings":["x"]}}]}}'));

SELECT '-- a database has no reset, so the command keeps the modify syntax and is rejected on execution';
SELECT formatQuerySingleLine('ALTER DATABASE d MODIFY SETTING x = DEFAULT');
