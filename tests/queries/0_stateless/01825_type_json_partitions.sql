-- Tags: no-fasttest

DROP TABLE IF EXISTS t_json_partitions;

SET allow_experimental_object_type = 1;
SET output_format_json_named_tuples_as_objects = 1;

CREATE TABLE t_json_partitions (id UInt32, obj Object('json'))
ENGINE MergeTree ORDER BY id PARTITION BY id;

INSERT INTO t_json_partitions FORMAT JSONEachRow {"id": 1, "obj": {"k1": "v1"}} {"id": 2, "obj": {"k2": "v2"}};

SELECT * FROM t_json_partitions ORDER BY id FORMAT JSONEachRow;

DROP TABLE t_json_partitions;
