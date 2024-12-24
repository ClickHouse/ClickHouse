-- Tags: no-fasttest

SET allow_experimental_object_type = 1;


DROP TABLE IF EXISTS t_json_desc;

CREATE TABLE t_json_desc (data JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_desc FORMAT JSONAsObject {"k1": 10}
;

DESC TABLE t_json_desc;
DESC TABLE t_json_desc SETTINGS describe_extend_object_types = 1;

INSERT INTO t_json_desc FORMAT JSONAsObject {"k1": "q", "k2": [1, 2, 3]}
;

DESC TABLE t_json_desc SETTINGS describe_extend_object_types = 1;

DROP TABLE IF EXISTS t_json_desc;
