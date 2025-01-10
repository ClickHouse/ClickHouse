-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS t_json;

CREATE TABLE t_json(id UInt64, data Object('JSON'))
ENGINE = MergeTree ORDER BY tuple();

SYSTEM STOP MERGES t_json;

INSERT INTO t_json FORMAT JSONEachRow {"id": 1, "data": {"k1": "aa", "k2": {"k3": "bb", "k4": "c"}}} {"id": 2, "data": {"k1": "ee", "k5": "ff"}};
INSERT INTO t_json FORMAT JSONEachRow {"id": 3, "data": {"k5":"foo"}};

SELECT id, data.k1, data.k2.k3, data.k2.k4, data.k5 FROM t_json ORDER BY id;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

SYSTEM START MERGES t_json;

OPTIMIZE TABLE t_json FINAL;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

SELECT '============';
TRUNCATE TABLE t_json;

INSERT INTO t_json FORMAT JSONEachRow {"id": 1, "data": {"k1":[{"k2":"aaa","k3":[{"k4":"bbb"},{"k4":"ccc"}]},{"k2":"ddd","k3":[{"k4":"eee"},{"k4":"fff"}]}]}};
SELECT id, data.k1.k2, data.k1.k3.k4 FROM t_json ORDER BY id;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

SELECT '============';
TRUNCATE TABLE t_json;

SYSTEM STOP MERGES t_json;

INSERT INTO t_json FORMAT JSONEachRow {"id": 1, "data": {"name": "a", "value": 42 }}, {"id": 2, "data": {"name": "b", "value": 4200 }};

SELECT id, data.name, data.value FROM t_json ORDER BY id;
SELECT sum(data.value) FROM t_json;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

INSERT INTO t_json FORMAT JSONEachRow {"id": 3, "data": {"name": "a", "value": 42.123 }};

SELECT id, data.name, data.value FROM t_json ORDER BY id;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

INSERT INTO t_json FORMAT JSONEachRow {"id": 4, "data": {"name": "a", "value": "some" }};

SELECT id, data.name, data.value FROM t_json ORDER BY id;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

SYSTEM START MERGES t_json;
OPTIMIZE TABLE t_json FINAL;

SELECT name, column, type
FROM system.parts_columns
WHERE table = 't_json' AND database = currentDatabase() AND active AND column = 'data'
ORDER BY name;

DROP TABLE IF EXISTS t_json;

CREATE TABLE t_json(id UInt64, data Object('JSON')) ENGINE = Log; -- { serverError 44 }
