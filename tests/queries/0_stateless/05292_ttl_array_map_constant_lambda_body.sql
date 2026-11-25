-- A TTL expression that reads the result of `arrayMap` whose lambda returns a constant `Dynamic` value.

-- With `cast_keep_nullable`, `toUInt8` of a `Dynamic` value is `Nullable`, and a TTL expression cannot be `Nullable`.
SET cast_keep_nullable = 0;

CREATE TABLE t_ttl_array_map_length (d DateTime, arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple()
TTL d + toIntervalDay(length(arrayMap(x -> CAST(1, 'Dynamic'), arr)));
INSERT INTO t_ttl_array_map_length VALUES ('2100-01-01 00:00:00', [1]);
SELECT count() FROM t_ttl_array_map_length;

CREATE TABLE t_ttl_array_map_element (d DateTime, arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple()
TTL d + toIntervalDay(toUInt8(arrayMap(x -> CAST(1, 'Dynamic'), arr)[1]));
INSERT INTO t_ttl_array_map_element VALUES ('2100-01-01 00:00:00', [1]);
SELECT count() FROM t_ttl_array_map_element;

ALTER TABLE t_ttl_array_map_element MODIFY TTL d + toIntervalDay(toUInt8(arrayMap(x -> CAST(2, 'Dynamic'), arr)[1]));
SELECT count() FROM t_ttl_array_map_element;

DROP TABLE t_ttl_array_map_length;
DROP TABLE t_ttl_array_map_element;
