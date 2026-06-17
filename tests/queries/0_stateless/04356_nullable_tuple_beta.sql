-- Nullable(Tuple(...)) is a Beta feature gated by `enable_nullable_tuple_type`
-- (default off), with the old name `allow_experimental_nullable_tuple_type` kept as an alias.

DROP TABLE IF EXISTS test_nullable_tuple_beta;

-- Off by default: creation is rejected.
CREATE TABLE test_nullable_tuple_beta (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

-- Enabling via the new (Beta) name allows creation.
SET enable_nullable_tuple_type = 1;
CREATE TABLE test_nullable_tuple_beta (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_beta VALUES (NULL), (tuple(1, 2));
SELECT a IS NULL AS is_null, a FROM test_nullable_tuple_beta ORDER BY is_null;
DROP TABLE test_nullable_tuple_beta;

-- The old name is an alias for the same setting: clearing the new name and setting the
-- alias has the same effect.
SET enable_nullable_tuple_type = 0;
SET allow_experimental_nullable_tuple_type = 1;
SELECT value FROM system.settings WHERE name = 'enable_nullable_tuple_type';
CREATE TABLE test_nullable_tuple_beta (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory;
DROP TABLE test_nullable_tuple_beta;
