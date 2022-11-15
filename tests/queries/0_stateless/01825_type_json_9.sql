-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json;

SET allow_experimental_object_type = 1;

CREATE TABLE t_json(id UInt64, obj JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json format JSONEachRow {"id": 1, "obj": {"foo": 1, "k1": 2}};
INSERT INTO t_json format JSONEachRow {"id": 2, "obj": {"foo": 1, "k2": 2}};

OPTIMIZE TABLE t_json FINAL;

SELECT any(toTypeName(obj)) from t_json;

DROP TABLE IF EXISTS t_json;
