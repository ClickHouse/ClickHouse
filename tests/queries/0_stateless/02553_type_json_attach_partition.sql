SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS t_json_attach_partition;

CREATE TABLE t_json_attach_partition(b UInt64, c Object('json')) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_attach_partition FORMAT JSONEachRow {"b": 1, "c" : {"k1": 1}};

ALTER TABLE t_json_attach_partition DETACH PARTITION tuple();
INSERT INTO t_json_attach_partition FORMAT JSONEachRow {"b": 1, "c" : {"k1": [1, 2]}};

ALTER TABLE t_json_attach_partition ATTACH PARTITION tuple(); -- { serverError TYPE_MISMATCH }
SELECT * FROM t_json_attach_partition FORMAT JSONEachRow;

DROP TABLE t_json_attach_partition;
