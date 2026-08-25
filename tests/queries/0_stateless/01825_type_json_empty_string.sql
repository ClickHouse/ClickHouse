-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_empty_str;
SET allow_experimental_object_type = 1;

CREATE TABLE t_json_empty_str(id UInt32, o Object('json')) ENGINE = Memory;

INSERT INTO t_json_empty_str VALUES (1, ''), (2, '{"k1": 1, "k2": "v1"}'), (3, '{}'), (4, '{"k1": 2}');

SELECT * FROM t_json_empty_str ORDER BY id;

DROP TABLE t_json_empty_str;
