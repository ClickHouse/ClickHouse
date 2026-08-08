-- Tags: no-fasttest, no-parallel

DROP TABLE IF EXISTS t_json_17;
SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

CREATE TABLE t_json_17(obj Object('json'))
ENGINE = MergeTree ORDER BY tuple();

DROP FUNCTION IF EXISTS hasValidSizes17;
CREATE FUNCTION hasValidSizes17 AS (arr1, arr2) -> length(arr1) = length(arr2) AND arrayAll((x, y) -> length(x) = length(y), arr1, arr2);

SYSTEM STOP MERGES t_json_17;

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 1, "arr": [{"k1": [{"k2": "aaa", "k3": "bbb"}, {"k2": "ccc"}]}]}

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 2, "arr": [{"k1": [{"k3": "ddd", "k4": 10}, {"k4": 20}], "k5": {"k6": "foo"}}]}

SELECT toTypeName(obj) FROM t_json_17 LIMIT 1;
SELECT obj FROM t_json_17 ORDER BY obj.id FORMAT JSONEachRow;
SELECT obj.arr.k1.k3, obj.arr.k1.k2 FROM t_json_17 ORDER BY obj.id;
SELECT sum(hasValidSizes17(obj.arr.k1.k3, obj.arr.k1.k2)) == count() FROM t_json_17;
SELECT obj.arr.k1.k4 FROM t_json_17 ORDER BY obj.id;

TRUNCATE TABLE t_json_17;

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 1, "arr": [{"k1": [{"k2": "aaa"}]}]}

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 2, "arr": [{"k1": [{"k2": "bbb", "k3": [{"k4": 10}]}, {"k2": "ccc", "k3": [{"k4": 20}]}]}]}

SELECT toTypeName(obj) FROM t_json_17 LIMIT 1;
SELECT obj FROM t_json_17 ORDER BY obj.id FORMAT JSONEachRow;
SELECT obj.arr.k1.k2, obj.arr.k1.k3.k4 FROM t_json_17 ORDER BY obj.id;
SELECT sum(hasValidSizes17(obj.arr.k1.k2, obj.arr.k1.k3.k4)) == count() FROM t_json_17;
SELECT obj.arr.k1.k3.k4 FROM t_json_17 ORDER BY obj.id;

TRUNCATE TABLE t_json_17;

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 1, "arr": [{"k3": "qqq"}, {"k3": "www"}]}

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 2, "arr": [{"k1": [{"k2": "aaa"}], "k3": "eee"}]}

INSERT INTO t_json_17 FORMAT JSONAsObject {"id": 3, "arr": [{"k1": [{"k2": "bbb", "k4": [{"k5": 10}]}, {"k2": "ccc", "k4": [{"k5": 20}]}], "k3": "rrr"}]}

SELECT toTypeName(obj) FROM t_json_17 LIMIT 1;
SELECT obj FROM t_json_17 ORDER BY obj.id FORMAT JSONEachRow;
SELECT obj.arr.k3, obj.arr.k1.k2, obj.arr.k1.k4.k5 FROM t_json_17 ORDER BY obj.id;
SELECT sum(hasValidSizes17(obj.arr.k1.k2, obj.arr.k1.k4.k5)) == count() FROM t_json_17;
SELECT obj.arr.k1.k4.k5 FROM t_json_17 ORDER BY obj.id;

DROP FUNCTION hasValidSizes17;
DROP TABLE t_json_17;
