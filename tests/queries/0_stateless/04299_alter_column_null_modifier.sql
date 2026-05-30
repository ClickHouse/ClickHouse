-- Tests that ALTER TABLE ADD/MODIFY COLUMN supports the trailing NULL / NOT NULL
-- modifier, the same way CREATE TABLE does.
-- See https://github.com/ClickHouse/ClickHouse/issues/44752

DROP TABLE IF EXISTS t_alter_null;

CREATE TABLE t_alter_null (a Int32) ENGINE = Memory;

-- ADD COLUMN with the postfix NULL / NOT NULL modifier, for single-word and multiword types.
ALTER TABLE t_alter_null ADD COLUMN b Int32 NULL;
ALTER TABLE t_alter_null ADD COLUMN c Int32 NOT NULL;
ALTER TABLE t_alter_null ADD COLUMN d TINYINT UNSIGNED NULL;
ALTER TABLE t_alter_null ADD COLUMN e DOUBLE PRECISION NOT NULL;

SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null'
ORDER BY name;

-- MODIFY COLUMN with the postfix NULL modifier turns a non-nullable column nullable.
ALTER TABLE t_alter_null MODIFY COLUMN c Int32 NULL;

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null' AND name = 'c';

-- A [NOT] NULL modifier on an already-Nullable type is rejected, the same way as in CREATE TABLE.
ALTER TABLE t_alter_null ADD COLUMN f Nullable(Int32) NULL; -- { serverError ILLEGAL_SYNTAX_FOR_DATA_TYPE }

DROP TABLE t_alter_null;
