SET use_variant_as_common_type = 1;
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (json Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=1, write_marks_for_substreams_in_compact_parts=1, min_bytes_for_wide_part='100G';
INSERT INTO t0 SELECT toJSONString(map('a.b.d', CAST(number, 'UInt32'), 'a.b.e', concat('str_', toString(number)))) FROM numbers(3);
SELECT json.a.b.e, json.a.b.e.:Int64, json.^a FROM t0;
DROP TABLE t0;

