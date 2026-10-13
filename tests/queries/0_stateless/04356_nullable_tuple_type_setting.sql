-- `Nullable(Tuple(...))` columns are allowed by default. The canonical setting is
-- `allow_experimental_nullable_tuple_type`; `enable_nullable_tuple_type` is an alias for it.

DROP TABLE IF EXISTS test_nullable_tuple_setting;

-- On by default.
CREATE TABLE test_nullable_tuple_setting (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_setting VALUES (NULL), (tuple(1, 2));
SELECT a IS NULL AS is_null, a FROM test_nullable_tuple_setting ORDER BY is_null;
DROP TABLE test_nullable_tuple_setting;

-- Disabling through the alias resolves to the canonical setting, and creation is rejected.
SET enable_nullable_tuple_type = 0;
SELECT value FROM system.settings WHERE name = 'allow_experimental_nullable_tuple_type';
CREATE TABLE test_nullable_tuple_setting (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

-- The canonical name works too.
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE test_nullable_tuple_setting (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory;
DROP TABLE test_nullable_tuple_setting;
