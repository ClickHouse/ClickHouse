-- Tags: no-fasttest

SET enable_json_type = 1;
SET input_format_json_infer_array_of_dynamic_from_array_of_different_types = 0;

DROP TABLE IF EXISTS t_json_2;

CREATE TABLE t_json_2(id UInt64, data JSON)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01825_2/t_json_2', 'r1') ORDER BY tuple();

INSERT INTO t_json_2 FORMAT JSONEachRow {"id": 1, "data": {"k1": 1, "k2" : 2}} {"id": 2, "data": {"k2": 3, "k3" : 4}};

SELECT id, data, JSONAllPathsWithTypes(data) FROM t_json_2 ORDER BY id;
SELECT id, data.k1, data.k2, data.k3 FROM t_json_2 ORDER BY id;

INSERT INTO t_json_2 FORMAT JSONEachRow {"id": 3, "data": {"k3" : 10}} {"id": 4, "data": {"k2": 5, "k3" : "str"}};

SELECT id, data, JSONAllPathsWithTypes(data) FROM t_json_2 ORDER BY id;
SELECT id, data.k1, data.k2, data.k3 FROM t_json_2 ORDER BY id;

SELECT '============';
TRUNCATE TABLE t_json_2;

INSERT INTO TABLE t_json_2 FORMAT JSONEachRow {"id": 1, "data": {"k1" : [1, 2, 3.3]}};

SELECT id, data, JSONAllPathsWithTypes(data) FROM t_json_2 ORDER BY id;
SELECT id, data.k1 FROM t_json_2 ORDEr BY id;

INSERT INTO TABLE t_json_2 FORMAT JSONEachRow {"id": 2, "data": {"k1" : ["a", 4, "b"]}};

SELECT id, data, JSONAllPathsWithTypes(data) FROM t_json_2 ORDER BY id;
SELECT id, data.k1 FROM t_json_2 ORDER BY id;

SELECT '============';
TRUNCATE TABLE t_json_2;

INSERT INTO TABLE t_json_2 FORMAT JSONEachRow {"id": 1, "data": {"k1" : [{"k2" : 11}, {"k3" : 22}]}} {"id": 2, "data": {"k1" : [{"k3" : 33}, {"k4" : 44}, {"k3" : 55, "k4" : 66}]}};

SELECT id, data, JSONAllPathsWithTypes(data) FROM t_json_2 ORDER BY id;
SELECT id, data.k1.k2, data.k1.k3, data.k1.k4 FROM t_json_2 ORDER BY id;

DROP TABLE t_json_2;
