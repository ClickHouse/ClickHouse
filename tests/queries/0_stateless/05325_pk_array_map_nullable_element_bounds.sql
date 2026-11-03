-- Array and Map key values holding a NULL inside: a granule bounded by such a mark must not be skipped.

DROP TABLE IF EXISTS t_pk_array_nullable_element;
DROP TABLE IF EXISTS t_pk_map_nullable_value;

CREATE TABLE t_pk_array_nullable_element (a Array(Nullable(UInt8))) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_pk_array_nullable_element VALUES ([1, 5]), ([2, 3]), ([2, NULL]), ([3, 1]), ([3, NULL]), ([4, 1]);

CREATE TABLE t_pk_map_nullable_value (m Map(UInt8, Nullable(UInt8))) ENGINE = MergeTree ORDER BY m
SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_pk_map_nullable_value VALUES ({1: 5}), ({2: 3}), ({2: NULL}), ({3: 1}), ({3: NULL}), ({4: 1});

SET use_query_condition_cache = 0;

SET use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_pk_array_nullable_element WHERE a = [2, 3];
SELECT count() FROM t_pk_array_nullable_element WHERE a >= [2, 3];
SELECT count() FROM t_pk_map_nullable_value WHERE m = map(2, 3);

SET use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_pk_array_nullable_element WHERE a = [2, 3];
SELECT count() FROM t_pk_array_nullable_element WHERE a >= [2, 3];
SELECT count() FROM t_pk_map_nullable_value WHERE m = map(2, 3);

DROP TABLE t_pk_array_nullable_element;
DROP TABLE t_pk_map_nullable_value;
