-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_nullable_key;

-- A Nullable element anywhere inside a Row key requires `allow_nullable_key`.
CREATE TABLE row_nullable_key (r Row(x Nullable(UInt64), y UInt64)) ENGINE = MergeTree ORDER BY r; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE row_nullable_key (r Row(x Tuple(Nullable(UInt64)), y UInt64)) ENGINE = MergeTree ORDER BY r; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE row_nullable_key (r Row(x Row(z Nullable(UInt64)), y UInt64)) ENGINE = MergeTree ORDER BY r; -- { serverError ILLEGAL_COLUMN }

-- Without Nullable elements a Row key is allowed.
CREATE TABLE row_nullable_key (r Row(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY r;
INSERT INTO row_nullable_key VALUES ((2, 1)), ((1, 2));
SELECT r FROM row_nullable_key ORDER BY r;
DROP TABLE row_nullable_key;

-- With `allow_nullable_key` enabled a Nullable element is accepted.
CREATE TABLE row_nullable_key (r Row(x Nullable(UInt64), y UInt64)) ENGINE = MergeTree ORDER BY r SETTINGS allow_nullable_key = 1;
INSERT INTO row_nullable_key VALUES ((1, 2)), ((NULL, 3));
SELECT r FROM row_nullable_key ORDER BY r;
DROP TABLE row_nullable_key;
