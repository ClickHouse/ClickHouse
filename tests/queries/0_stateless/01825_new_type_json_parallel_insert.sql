-- Tags: long
DROP TABLE IF EXISTS t_json_parallel;

SET enable_json_type = 1, max_insert_threads = 20, max_threads = 20, min_insert_block_size_rows = 65536;
CREATE TABLE t_json_parallel (data JSON) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_json_parallel SELECT materialize('{"k1":1, "k2": "some"}') FROM numbers_mt(500000);
SELECT groupUniqArrayMap(JSONAllPathsWithTypes(data)), count() FROM t_json_parallel;

DROP TABLE t_json_parallel;
