-- Test hasTokenOrNull over an Enum haystack on the dictionary transform path with a
-- separator needle that yields NULL for every row: exercises the null-map expand step.
DROP TABLE IF EXISTS t_enum_null_expand;

CREATE TABLE t_enum_null_expand
(c Enum8('' = -128, 'a' = -5, 'A' = 0, 'AB' = 1, 'aBc' = 2, 'ABCD' = 3, 'xAyz' = 100, 'Foo A Bar' = 127))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_enum_null_expand
SELECT ['', 'a', 'A', 'AB', 'aBc', 'ABCD', 'xAyz', 'Foo A Bar'][(number % 8) + 1] FROM numbers(1000);

-- Separator needle -> every row NULL under the Null execution policy; must match toString path.
SELECT 'null status matches toString',
       sum((hasTokenOrNull(c, 'A B') IS NULL) != (hasTokenOrNull(toString(c), 'A B') IS NULL)) FROM t_enum_null_expand;
SELECT 'nulls mapped to every row', countIf(hasTokenOrNull(c, 'A B') IS NULL), count() FROM t_enum_null_expand;

-- Non-separator needle -> no NULLs; result value must match toString path.
SELECT 'non-null value matches toString',
       sum(assumeNotNull(hasTokenOrNull(c, 'A')) != assumeNotNull(hasTokenOrNull(toString(c), 'A'))) FROM t_enum_null_expand;

DROP TABLE t_enum_null_expand;
