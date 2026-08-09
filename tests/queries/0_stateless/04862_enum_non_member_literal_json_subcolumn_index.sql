-- A non-member Enum literal compared against a JSON subcolumn cast must not throw when the table
-- carries a JSONAllPaths index; the index shares one conversion helper with the bloom filter family.

SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS t_json_bf;
DROP TABLE IF EXISTS t_json_tokenbf;

CREATE TABLE t_json_bf      (j JSON, v UInt64, INDEX i JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_json_tokenbf (j JSON, v UInt64, INDEX i JSONAllPaths(j) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_json_bf      VALUES ('{"a":"x"}', 1), ('{"a":"y"}', 2);
INSERT INTO t_json_tokenbf VALUES ('{"a":"x"}', 1), ('{"a":"y"}', 2);

-- Each predicate sits in a WHERE clause: only there does index analysis run, and it is index
-- analysis that used to throw. The same predicate inside countIf() never reaches it.
SELECT 'bloom_filter';
SELECT count() FROM t_json_bf;
SELECT count() FROM t_json_bf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'x';
SELECT count() FROM t_json_bf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'x' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_bf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'zzz';
SELECT count() FROM t_json_bf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'zzz' SETTINGS use_skip_indexes = 0;

SELECT 'tokenbf_v1';
SELECT count() FROM t_json_tokenbf;
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'x';
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'x' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'zzz';
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'zzz' SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_bf;
DROP TABLE t_json_tokenbf;
