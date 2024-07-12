SET output_format_json_named_tuples_as_objects = 1;
SET allow_experimental_object_type = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_json_analyzer;
CREATE TABLE t_json_analyzer (a JSON) ENGINE = Memory;
INSERT INTO t_json_analyzer VALUES ('{"id": 2, "obj": {"k2": {"k3": "str", "k4": [{"k6": 55}]}, "some": 42}, "s": "bar"}');

SELECT any(a) AS data FROM t_json_analyzer FORMAT JSONEachRow;
SELECT flattenTuple(a) AS data FROM t_json_analyzer FORMAT JSONEachRow;
SELECT  a.id, a.obj.* FROM t_json_analyzer FORMAT JSONEachRow;

DROP TABLE t_json_analyzer;
