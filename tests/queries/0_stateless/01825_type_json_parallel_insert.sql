-- Tags: long, no-backward-compatibility-check:22.3.2.1
DROP TABLE IF EXISTS t_json_parallel;

SET allow_experimental_object_type = 1, max_insert_threads = 20, max_threads = 20;
CREATE TABLE t_json_parallel (data JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_parallel SELECT materialize('{"k1":1, "k2": "some"}') FROM numbers_mt(3000000);
SELECT any(toTypeName(data)), count() FROM t_json_parallel;

DROP TABLE t_json_parallel;
