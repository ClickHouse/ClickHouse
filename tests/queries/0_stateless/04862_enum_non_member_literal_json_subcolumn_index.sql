-- Tags: no-fasttest, no-parallel-replicas
-- A non-member Enum literal compared against a JSON subcolumn cast must not throw when the table
-- carries a JSONAllPaths index; the index shares one conversion helper with the bloom filter family.

DROP TABLE IF EXISTS t_json_bf;
DROP TABLE IF EXISTS t_json_tokenbf;
DROP TABLE IF EXISTS t_json_num;

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

-- force_data_skipping_indices asserts index usability directly: a result count alone cannot
-- distinguish "declines this literal" from "declines everything", and isJSONPathFilterSafe gained
-- two new ways to return false.
-- 'x' is the Enum's default value, which the helper declines by a pre-existing rule
-- (converted == getDefault()), so 'y' is the literal that exercises the usable path.
SELECT 'a representable non-default literal still uses each index';
SELECT count() FROM t_json_bf      WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'y' SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'y' SETTINGS force_data_skipping_indices = 'i';

SELECT 'a non-member literal declines the index instead of throwing';
-- enable_analyzer = 1: the old analyzer folds a non-member Enum comparison to a constant false and
-- drops the MergeTree read, so no index analysis runs to assert on.
SELECT count() FROM t_json_bf      WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'zzz' SETTINGS force_data_skipping_indices = 'i', enable_analyzer = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS Enum8('x' = 1, 'y' = 2)) = 'zzz' SETTINGS force_data_skipping_indices = 'i', enable_analyzer = 1; -- { serverError INDEX_NOT_USED }

-- The helper is shared by every key type, so the widened null check needs a non-Enum witness:
-- these would redline if it started declining representable non-Enum predicates.
SELECT 'non-Enum cast targets keep using the index';
SELECT count() FROM t_json_bf      WHERE CAST(j.a AS String) = 'y' SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_json_tokenbf WHERE CAST(j.a AS String) = 'y' SETTINGS force_data_skipping_indices = 'i';
CREATE TABLE t_json_num (j JSON, v UInt64, INDEX i JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_json_num VALUES ('{"a":7}', 1), ('{"a":9}', 2);
SELECT count() FROM t_json_num WHERE CAST(j.a AS UInt8) = 9 SETTINGS force_data_skipping_indices = 'i';
SELECT count() FROM t_json_num WHERE CAST(j.a AS UInt8) = 9;
-- A literal outside the cast target's range converts to a null Field without throwing, which the
-- null check treats as "cannot skip". The row count is 0 either way; only index usability changes.
SELECT count() FROM t_json_num WHERE CAST(j.a AS UInt8) = 999 SETTINGS force_data_skipping_indices = 'i'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_json_num WHERE CAST(j.a AS UInt8) = 999;

DROP TABLE t_json_bf;
DROP TABLE t_json_tokenbf;
DROP TABLE t_json_num;
