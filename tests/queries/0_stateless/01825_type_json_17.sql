-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_17;
SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

CREATE TABLE t_json_17(obj JSON)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 1, "arr": [{"k1": [{"k2": "aaa", "k3": "bbb"}, {"k2": "ccc"}]}]}
INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 2, "arr": [{"k1": [{"k3": "ddd", "k4": 10}, {"k4": 20}], "k5": {"k6": "foo"}}]}

SELECT toTypeName(obj) FROM t_json_17 LIMIT 1;
SELECT obj FROM t_json_17 ORDER BY obj.id FORMAT JSONEachRow;
SELECT obj.arr.k1.k3, obj.arr.k1.k2 FROM t_json_17 ORDER BY obj.id;
SELECT obj.arr.k1.k4 FROM t_json_17 ORDER BY obj.id;

DROP TABLE IF EXISTS t_json_17;
