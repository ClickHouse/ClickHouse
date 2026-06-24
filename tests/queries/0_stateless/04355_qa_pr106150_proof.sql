-- Test that a type-less ALTER ADD COLUMN ... DEFAULT <expr> NULL applies the trailing
-- NULL modifier, mirroring CREATE TABLE (which makes such a column Nullable).

DROP TABLE IF EXISTS t_alter_null_default;

-- CREATE TABLE reference: a type-less DEFAULT column with a trailing NULL / NOT NULL.
CREATE TABLE t_alter_null_default (id_def DEFAULT 1 NULL, id_def_nn DEFAULT 1 NOT NULL)
ENGINE = MergeTree ORDER BY tuple();
SELECT 'create';
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null_default'
  AND name IN ('id_def', 'id_def_nn')
ORDER BY name;
DROP TABLE t_alter_null_default;

-- ALTER ADD COLUMN of a type-less DEFAULT column must mirror CREATE TABLE.
CREATE TABLE t_alter_null_default (a Int32) ENGINE = Memory;
ALTER TABLE t_alter_null_default ADD COLUMN x DEFAULT 1 NULL;
ALTER TABLE t_alter_null_default ADD COLUMN y DEFAULT 1 NOT NULL;
SELECT 'alter_add';
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null_default'
  AND name IN ('x', 'y')
ORDER BY name;

DROP TABLE IF EXISTS t_alter_null_default;
