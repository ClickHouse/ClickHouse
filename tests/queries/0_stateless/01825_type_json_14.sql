-- Tags: no-fasttest

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

DROP TABLE IF EXISTS t_json_14;
CREATE TABLE t_json_14 (id UInt32, o JSON) ENGINE = Memory;

INSERT INTO t_json_14 VALUES (1, '{"key_10":65536,"key_11":"anve","key_0":{"key_1":{"key_2":1025,"key_3":1},"key_4":1,"key_5":256}}');
INSERT INTO t_json_14 VALUES (2, '{"key_0":[{"key_12":"buwvq","key_11":0.0000000255}]}'); -- { serverError INCOMPATIBLE_COLUMNS }

SELECT * FROM t_json_14 ORDER BY id FORMAT JSONEachRow;

DROP TABLE t_json_14;
