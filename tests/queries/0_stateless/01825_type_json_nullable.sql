-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS t_json_null;

CREATE TABLE t_json_null(id UInt64, data Object(Nullable('JSON')))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_null FORMAT JSONEachRow {"id": 1, "data": {"k1": 1, "k2" : 2}} {"id": 2, "data": {"k2": 3, "k3" : 4}};

SELECT id, data, toTypeName(data) FROM t_json_null ORDER BY id;
SELECT id, data.k1, data.k2, data.k3 FROM t_json_null ORDER BY id;

INSERT INTO t_json_null FORMAT JSONEachRow {"id": 3, "data": {"k3" : 10}} {"id": 4, "data": {"k2": 5, "k3" : "str"}};

SELECT id, data, toTypeName(data) FROM t_json_null ORDER BY id;
SELECT id, data.k1, data.k2, data.k3 FROM t_json_null ORDER BY id;

SELECT '============';
TRUNCATE TABLE t_json_null;

INSERT INTO TABLE t_json_null FORMAT JSONEachRow {"id": 1, "data": {"k1" : [{"k2" : 11}, {"k3" : 22}]}} {"id": 2, "data": {"k1" : [{"k3" : 33}, {"k4" : 44}, {"k3" : 55, "k4" : 66}]}};

SELECT id, data, toTypeName(data) FROM t_json_null ORDER BY id;
SELECT id, data.k1.k2, data.k1.k3, data.k1.k4 FROM t_json_null ORDER BY id;

DROP TABLE t_json_null;
