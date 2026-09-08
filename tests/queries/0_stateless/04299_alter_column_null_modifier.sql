-- Tests that ALTER TABLE ADD / MODIFY / ALTER COLUMN supports the trailing NULL / NOT NULL
-- modifier, the same way CREATE TABLE does, for single-word and multiword types.
-- The modifier needs an explicit column type to apply to, so ADD COLUMN, MODIFY COLUMN
-- and ALTER COLUMN ... TYPE all accept it; only a type-less MODIFY / ALTER COLUMN keeps
-- rejecting it at parse time (covered by 02302_column_decl_null_before_defaul_value).
-- See https://github.com/ClickHouse/ClickHouse/issues/44752

DROP TABLE IF EXISTS t_alter_null;

CREATE TABLE t_alter_null (a Int32) ENGINE = Memory;

-- ADD COLUMN with the postfix NULL / NOT NULL modifier, for single-word and multiword types.
ALTER TABLE t_alter_null ADD COLUMN b Int32 NULL;
ALTER TABLE t_alter_null ADD COLUMN c Int32 NOT NULL;
ALTER TABLE t_alter_null ADD COLUMN d TINYINT UNSIGNED NULL;
ALTER TABLE t_alter_null ADD COLUMN e DOUBLE PRECISION NOT NULL;

SELECT 'add';
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null'
ORDER BY name;

-- MODIFY COLUMN with the postfix modifier, single-word and multiword types, both directions.
ALTER TABLE t_alter_null MODIFY COLUMN c Int32 NULL;            -- Int32 -> Nullable(Int32)
ALTER TABLE t_alter_null MODIFY COLUMN b Int32 NOT NULL;        -- Nullable(Int32) -> Int32
ALTER TABLE t_alter_null MODIFY COLUMN e DOUBLE PRECISION NULL; -- Float64 -> Nullable(Float64)

SELECT 'modify';
SELECT name, type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null'
ORDER BY name;

-- ALTER COLUMN ... TYPE ... NULL is supported too (same effect as MODIFY COLUMN ... NULL).
ALTER TABLE t_alter_null ALTER COLUMN a TYPE Int32 NULL;

SELECT 'alter_column';
SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = 't_alter_null' AND name = 'a';

-- A [NOT] NULL modifier on an already-Nullable type is rejected, the same way as in CREATE TABLE.
ALTER TABLE t_alter_null ADD COLUMN f Nullable(Int32) NULL; -- { serverError ILLEGAL_SYNTAX_FOR_DATA_TYPE }
ALTER TABLE t_alter_null MODIFY COLUMN c Nullable(Int32) NULL; -- { serverError ILLEGAL_SYNTAX_FOR_DATA_TYPE }

-- A type that cannot be nested inside Nullable is rejected, the same way as in CREATE TABLE.
ALTER TABLE t_alter_null ADD COLUMN g Array(Int32) NULL; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t_alter_null MODIFY COLUMN a Array(Int32) NULL; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_alter_null;
