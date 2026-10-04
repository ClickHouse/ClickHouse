-- `if`, `ifNull` and `coalesce` must not convert the values under `NULL` of their argument to the result type:
-- such a value can be invalid, like `0` for an `Enum` that has no such element.

DROP TABLE IF EXISTS t_enum_null;
CREATE TABLE t_enum_null (y Nullable(Enum8('z' = 1, 'a' = 2))) ENGINE = Memory;
INSERT INTO t_enum_null SETTINGS async_insert = 0 VALUES ('a'), (NULL);

SELECT if(isNotNull(y), y, '') FROM t_enum_null ORDER BY ALL;
SELECT if(isNull(y), '', y) FROM t_enum_null ORDER BY ALL;
SELECT ifNull(y, '') FROM t_enum_null ORDER BY ALL;
SELECT coalesce(y, '') FROM t_enum_null ORDER BY ALL;
SELECT coalesce(y, y, 'x') FROM t_enum_null ORDER BY ALL;

-- The same inside a sorting key, computed on insert.
DROP TABLE IF EXISTS t_enum_null_key;
CREATE TABLE t_enum_null_key (y Nullable(Enum8('z' = 1, 'a' = 2))) ENGINE = MergeTree ORDER BY ifNull(y, '');
INSERT INTO t_enum_null_key SETTINGS async_insert = 0 VALUES ('a'), (NULL);
SELECT y FROM t_enum_null_key ORDER BY ALL;

DROP TABLE t_enum_null;
DROP TABLE t_enum_null_key;
