-- https://github.com/ClickHouse/ClickHouse/issues/116407
-- The `bloom_filter` index hashes the exact bytes of a constant, while the comparison it stands in
-- for is padding-aware for the string family. Two paths still hashed the wrong byte length and pruned
-- every granule: a set coming from a subquery or a table (which keeps its own element type, unlike a
-- literal `IN` list), and `m['k'] = const` over a `mapValues` index (hashed without coercing the
-- constant to the map's value type).

DROP TABLE IF EXISTS t_bf_set;
CREATE TABLE t_bf_set (v String, INDEX bf v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_set VALUES ('V0'), ('V0\0'), ('V0\0\0'), ('other1'), ('other2');
OPTIMIZE TABLE t_bf_set FINAL;

SELECT 'ground truth';
SELECT 'V0\0' = 'V0'::FixedString(3);

SELECT 'subquery set';
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0'::FixedString(3));
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0'::FixedString(3)) SETTINGS use_skip_indexes = 0;

SELECT 'literal set';
SELECT count() FROM t_bf_set WHERE v IN ('V0'::FixedString(3));
SELECT count() FROM t_bf_set WHERE v IN ('V0'::FixedString(3)) SETTINGS use_skip_indexes = 0;

SELECT 'same-type subquery set still prunes';
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0');
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'V0') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_set WHERE v IN (SELECT 'nosuch');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_set WHERE v IN (SELECT 'nosuch')) WHERE explain LIKE '%Granules: 0/%';

SELECT 'map value';
DROP TABLE IF EXISTS t_bf_map;
CREATE TABLE t_bf_map (m Map(String, FixedString(3)), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map VALUES (map('k', 'V0')), (map('k', 'ab')), (map('k', 'xyz'));
OPTIMIZE TABLE t_bf_map FINAL;

SELECT count() FROM t_bf_map WHERE m['k'] = 'V0';
SELECT count() FROM t_bf_map WHERE m['k'] = 'V0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map WHERE m['k'] = 'xyz';
SELECT count() FROM t_bf_map WHERE m['k'] = 'xyz' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bf_map WHERE m['k'] = 'nos';
SELECT count() FROM t_bf_map WHERE m['k'] = 'nos' SETTINGS use_skip_indexes = 0;

SELECT 'map value from a subquery set';
-- The `mapValues` arm of the set path casts the set column to the map's value type the same way, so a
-- `FixedString` element over a `String` value type hashes the stripped bytes and prunes every granule.
DROP TABLE IF EXISTS t_bf_map_str;
CREATE TABLE t_bf_map_str (m Map(String, String), INDEX bf mapValues(m) TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_bf_map_str VALUES (map('k', 'V0')), (map('k', 'V0\0')), (map('k', 'xx'));
OPTIMIZE TABLE t_bf_map_str FINAL;

SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'V0'::FixedString(3));
SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'V0'::FixedString(3)) SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_bf_map_set_source;
CREATE TABLE t_bf_map_set_source (v FixedString(3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_map_set_source VALUES ('V0');

SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT v FROM t_bf_map_set_source);
SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT v FROM t_bf_map_set_source) SETTINGS use_skip_indexes = 0;

SELECT 'same-type subquery set over a map still prunes';
SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'nosuch');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bf_map_str WHERE m['k'] IN (SELECT 'nosuch')) WHERE explain LIKE '%Granules: 0/%';

DROP TABLE t_bf_set;
DROP TABLE t_bf_map;
DROP TABLE t_bf_map_str;
DROP TABLE t_bf_map_set_source;
