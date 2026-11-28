-- The source subcolumn is stored in a separate stream in MergeTree parts.

DROP TABLE IF EXISTS t_json_source_mt;
CREATE TABLE t_json_source_mt (id UInt64, json JSON(with_source=1, a UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, object_serialization_version = 'v3', object_shared_data_serialization_version = 'map_with_buckets';

INSERT INTO t_json_source_mt VALUES (1, '{"a" : 42, "b" : "Hello"}'), (2, '{"a" : 43, "c" : [1, 2, 3]}');
INSERT INTO t_json_source_mt VALUES (3, '{"a" :  44,  "d" : "2020-01-01"}');

SELECT 'wide part';
SELECT id, json.__source FROM t_json_source_mt ORDER BY id;
SELECT id, json, json.a FROM t_json_source_mt ORDER BY id;
SELECT name FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_json_source_mt' AND active AND column = 'json' AND has(subcolumns.names, '__source') ORDER BY name FORMAT Null;

OPTIMIZE TABLE t_json_source_mt FINAL;
SELECT 'after merge';
SELECT id, json.__source FROM t_json_source_mt ORDER BY id;

DROP TABLE t_json_source_mt;

DROP TABLE IF EXISTS t_json_source_mt_compact;
CREATE TABLE t_json_source_mt_compact (id UInt64, json JSON(with_source=1))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;

INSERT INTO t_json_source_mt_compact VALUES (1, '{"a" : 42}'), (2, '{"b" : [1, 2, 3]}');
SELECT 'compact part';
SELECT id, json.__source FROM t_json_source_mt_compact ORDER BY id;
SELECT id, json FROM t_json_source_mt_compact ORDER BY id;

DROP TABLE t_json_source_mt_compact;
