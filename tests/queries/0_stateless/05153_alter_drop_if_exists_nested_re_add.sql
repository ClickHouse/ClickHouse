-- Regression: the conditional form `DROP COLUMN IF EXISTS n` must recognize a Nested parent that
-- physically stores flattened members (`n.x` / `n.y`). `AlterCommand::apply` guarded the skip with
-- the exact-only `ColumnsDescription::has(n)`, so the DROP returned early, and a same-statement
-- `DROP COLUMN IF EXISTS n, ADD COLUMN IF NOT EXISTS n.x` re-added against unmodified metadata and
-- became a silent no-op -- unlike the unconditional `DROP COLUMN n` fixed in 05059.

DROP TABLE IF EXISTS alter_drop_if_exists_nested_re_add;
CREATE TABLE alter_drop_if_exists_nested_re_add (a Int64, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO alter_drop_if_exists_nested_re_add VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE alter_drop_if_exists_nested_re_add (DROP COLUMN IF EXISTS n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 'if-exists nested re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'alter_drop_if_exists_nested_re_add' ORDER BY name;
SELECT 'if-exists nested re-add data', a, n.x FROM alter_drop_if_exists_nested_re_add ORDER BY a;

DROP TABLE alter_drop_if_exists_nested_re_add;

-- The skip must still fire for a genuinely absent name: a scalar name and a Nested parent with no
-- members left are both "not exists", and the statement must be a no-op.
DROP TABLE IF EXISTS alter_drop_if_exists_missing;
CREATE TABLE alter_drop_if_exists_missing (a Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO alter_drop_if_exists_missing VALUES (1);

ALTER TABLE alter_drop_if_exists_missing (DROP COLUMN IF EXISTS b), (DROP COLUMN IF EXISTS n);
SELECT 'missing skip columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'alter_drop_if_exists_missing' ORDER BY name;
SELECT 'missing skip data', a FROM alter_drop_if_exists_missing ORDER BY a;

DROP TABLE alter_drop_if_exists_missing;
