-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_bools;
SET allow_experimental_object_type = 1;

CREATE TABLE t_json_bools (data JSON) ENGINE = Memory;
INSERT INTO t_json_bools VALUES ('{"k1": true, "k2": false}');
SELECT data, toTypeName(data) FROM t_json_bools;

DROP TABLE t_json_bools;
