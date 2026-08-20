-- Nullable(Tuple(...)) is a Beta feature. The canonical setting is
-- `allow_experimental_nullable_tuple_type` (default off); `enable_nullable_tuple_type` is an alias for it.

DROP TABLE IF EXISTS test_nullable_tuple_beta;

-- Off by default: creation is rejected.
CREATE TABLE test_nullable_tuple_beta (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

-- Setting the alias resolves to the canonical setting.
SET enable_nullable_tuple_type = 1;
SELECT value FROM system.settings WHERE name = 'allow_experimental_nullable_tuple_type';

CREATE TABLE test_nullable_tuple_beta (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory;
INSERT INTO test_nullable_tuple_beta VALUES (NULL), (tuple(1, 2));
SELECT a IS NULL AS is_null, a FROM test_nullable_tuple_beta ORDER BY is_null;
DROP TABLE test_nullable_tuple_beta;

-- The canonical name works too.
SET enable_nullable_tuple_type = 0;
SET allow_experimental_nullable_tuple_type = 1;
CREATE TABLE test_nullable_tuple_beta (a Nullable(Tuple(b Int32, c Int32))) ENGINE = Memory;
DROP TABLE test_nullable_tuple_beta;
