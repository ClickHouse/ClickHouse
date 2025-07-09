-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_bools;
SET allow_experimental_json_type = 1;

CREATE TABLE t_json_bools (data JSON) ENGINE = Memory;
INSERT INTO t_json_bools VALUES ('{"k1": true, "k2": false}');
SELECT data, JSONAllPathsWithTypes(data) FROM t_json_bools;

DROP TABLE t_json_bools;
