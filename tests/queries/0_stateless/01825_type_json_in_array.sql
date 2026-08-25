-- Tags: no-fasttest

SET allow_experimental_object_type = 1;
DROP TABLE IF EXISTS t_json_array;

CREATE TABLE t_json_array (id UInt32, arr Array(Object('json'))) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_json_array FORMAT JSONEachRow {"id": 1, "arr": [{"k1": 1, "k2": {"k3": 2, "k4": 3}}, {"k1": 2, "k2": {"k5": "foo"}}]}

INSERT INTO t_json_array FORMAT JSONEachRow {"id": 2, "arr": [{"k1": 3, "k2": {"k3": 4, "k4": 5}}]}

SET output_format_json_named_tuples_as_objects = 1;

SELECT * FROM t_json_array ORDER BY id FORMAT JSONEachRow;
SELECT id, arr.k1, arr.k2.k3, arr.k2.k4, arr.k2.k5 FROM t_json_array ORDER BY id;
SELECT arr FROM t_json_array ARRAY JOIN arr ORDER BY arr.k1 FORMAT JSONEachRow;
SELECT toTypeName(arr) FROM t_json_array LIMIT 1;

TRUNCATE TABLE t_json_array;

INSERT INTO t_json_array FORMAT JSONEachRow {"id": 1, "arr": [{"k1": [{"k2": "aaa", "k3": "bbb"}, {"k2": "ccc"}]}]}

INSERT INTO t_json_array FORMAT JSONEachRow {"id": 2, "arr": [{"k1": [{"k3": "ddd", "k4": 10}, {"k4": 20}], "k5": {"k6": "foo"}}]}

SELECT * FROM t_json_array ORDER BY id FORMAT JSONEachRow;
SELECT id, arr.k1.k2, arr.k1.k3, arr.k1.k4, arr.k5.k6 FROM t_json_array ORDER BY id;

SELECT arrayJoin(arrayJoin(arr.k1)) AS k1 FROM t_json_array ORDER BY k1 FORMAT JSONEachRow;
SELECT toTypeName(arrayJoin(arrayJoin(arr.k1))) AS arr FROM t_json_array LIMIT 1;

DROP TABLE t_json_array;

SELECT * FROM values('arr Array(Object(''json''))', '[\'{"x" : 1}\']') FORMAT JSONEachRow;
SELECT * FROM values('arr Map(String, Object(''json''))', '{\'x\' : \'{"y" : 1}\', \'t\' : \'{"y" : 2}\'}') FORMAT JSONEachRow;
SELECT * FROM values('arr Tuple(Int32, Object(''json''))', '(1, \'{"y" : 1}\')', '(2, \'{"y" : 2}\')') FORMAT JSONEachRow;
SELECT * FROM format(JSONEachRow, '{"arr" : [{"x" : "aaa", "y" : [1,2,3]}]}') FORMAT JSONEachRow;
SELECT * FROM values('arr Array(Object(''json''))', '[\'{"x" : 1}\']') FORMAT JSONEachRow;
