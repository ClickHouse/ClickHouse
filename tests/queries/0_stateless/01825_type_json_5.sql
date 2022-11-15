-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

SELECT '{"a": {"b": 1, "c": 2}}'::JSON AS s;
SELECT '{"a": {"b": 1, "c": 2}}'::JSON AS s format JSONEachRow;

DROP TABLE IF EXISTS t_json_5;
DROP TABLE IF EXISTS t_json_str_5;

CREATE TABLE t_json_str_5 (data String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_json_5 (data JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_str_5 FORMAT JSONAsString {"k1": 1, "k2": {"k4": [22, 33]}}, {"k1": 2, "k2": {"k3": "qqq", "k4": [44]}}
;

INSERT INTO t_json_5 SELECT data FROM t_json_str_5;

SELECT data.k1, data.k2.k3, data.k2.k4 FROM t_json_5 ORDER BY data.k1;
SELECT DISTINCT toTypeName(data) FROM t_json_5;

DROP TABLE t_json_5;
DROP TABLE t_json_str_5;
