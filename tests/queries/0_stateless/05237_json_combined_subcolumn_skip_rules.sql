SET enable_json_type = 1;
SET allow_experimental_json_type = 1;

CREATE TABLE t_skip_path
(
    json JSON(a Nullable(Int64), a.b Nullable(Int64), SKIP b)
)
ENGINE = Memory;

INSERT INTO t_skip_path VALUES ('{"a.b": 42}');

SELECT 'skip path';
SELECT JSONHas(json, 'a'), JSONExtractRaw(json, 'a')
FROM t_skip_path
SETTINGS type_json_skip_null_typed_paths = 1;

CREATE TABLE t_skip_regexp
(
    json JSON(a Nullable(Int64), a.b Nullable(Int64), SKIP REGEXP '^b$')
)
ENGINE = Memory;

INSERT INTO t_skip_regexp VALUES ('{"a.b": 42}');

SELECT 'skip regexp';
SELECT JSONHas(json, 'a'), JSONExtractRaw(json, 'a')
FROM t_skip_regexp
SETTINGS type_json_skip_null_typed_paths = 1;

DROP TABLE t_skip_path;
DROP TABLE t_skip_regexp;
