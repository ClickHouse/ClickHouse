-- A `JSONAllPaths` skip index reports a declared typed path of a `JSON` type verbatim and never
-- recurses into it: a `JSON(a JSON)` column holding `{"a":{"b":42}}` has the granule path set `['a']`.
-- A filter on a subcolumn that reaches inside such a typed path (`json.a.b`, whose path is `a.b`) was
-- matched against that path set, found no granule containing `a.b` and pruned all of them, silently
-- returning no rows. The index is not consulted for such a subcolumn any more, which
-- `force_data_skipping_indices` shows below.

SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_05198_nested;
CREATE TABLE t_05198_nested (json JSON(a JSON), INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_05198_nested SELECT '{"a":{"b":42}}' FROM numbers(4);

SELECT 'the granule paths', arraySort(groupUniqArrayArray(JSONAllPaths(json))) FROM t_05198_nested;
SELECT 'a nested JSON typed path', count() FROM t_05198_nested WHERE json.a.b = 42;
SELECT 'the same without the index', count() FROM t_05198_nested WHERE json.a.b = 42 SETTINGS use_skip_indexes = 0;
SELECT 'isNotNull', count() FROM t_05198_nested WHERE isNotNull(json.a.b);
SELECT 'IN', count() FROM t_05198_nested WHERE json.a.b::Int64 IN (42, 43);
SELECT 'a cast', count() FROM t_05198_nested WHERE json.a.b::Int64 = 42;
SELECT count() FROM t_05198_nested WHERE json.a.b = 42 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_05198_tuple;
CREATE TABLE t_05198_tuple (json JSON(a Tuple(b Int64)), INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_05198_tuple SELECT '{"a":{"b":42}}' FROM numbers(4);

SELECT 'a Tuple typed path', count() FROM t_05198_tuple WHERE json.a.b = 42;
SELECT 'the same without the index', count() FROM t_05198_tuple WHERE json.a.b = 42 SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_05198_tokenbf;
CREATE TABLE t_05198_tokenbf (json JSON(a JSON), INDEX idx JSONAllPaths(json) TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_05198_tokenbf SELECT '{"a":{"b":42}}' FROM numbers(4);

SELECT 'a token bloom filter', count() FROM t_05198_tokenbf WHERE json.a.b = 42;
SELECT 'the same without the index', count() FROM t_05198_tokenbf WHERE json.a.b = 42 SETTINGS use_skip_indexes = 0;

-- The shapes the index is designed for keep using it: a scalar typed path is reported by its own
-- name, and a dynamic path is reported with all of its components.
DROP TABLE IF EXISTS t_05198_scalar;
CREATE TABLE t_05198_scalar (json JSON(x Int64), INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_05198_scalar SELECT '{"x":42}' FROM numbers(4);

SELECT 'a scalar typed path', count() FROM t_05198_scalar WHERE json.x = 42 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_05198_dynamic;
CREATE TABLE t_05198_dynamic (json JSON, INDEX idx JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_05198_dynamic SELECT '{"a":{"b":42}}' FROM numbers(4);

SELECT 'the dynamic granule paths', arraySort(groupUniqArrayArray(JSONAllPaths(json))) FROM t_05198_dynamic;
SELECT 'a nested dynamic path', count() FROM t_05198_dynamic WHERE json.a.b = 42 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'an absent path is still pruned', count() FROM t_05198_dynamic WHERE json.absent = 42 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_05198_dynamic;
DROP TABLE t_05198_scalar;
DROP TABLE t_05198_tokenbf;
DROP TABLE t_05198_tuple;
DROP TABLE t_05198_nested;
