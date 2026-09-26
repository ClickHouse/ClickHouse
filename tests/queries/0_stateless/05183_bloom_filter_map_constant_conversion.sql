-- The map branches of the `bloom_filter` index condition hash the comparison constant as a value of the
-- indexed type, but the constant does not arrive as one: comparing an `Enum`, a `DateTime` or a `UUID` with
-- a string literal keeps the literal a `String`, see `FunctionComparison::executeWithConstString`, and an
-- `arrayElement` map key stays the literal the query wrote. Hashing it as it arrived made
-- `BloomFilterHash::hashWithField` read a `String` field as the indexed type and throw `BAD_GET`, so the
-- query failed only because the map happened to be indexed. `optimize_functions_to_subcolumns` decides
-- whether the atom is a `map.key_<key>` subcolumn, whose key is deserialized with the key type, or an
-- `arrayElement` call, whose key is not - both must answer like the unindexed table.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_bloom_filter_map_constant;
DROP TABLE IF EXISTS t_bloom_filter_map_constant_plain;

CREATE TABLE t_bloom_filter_map_constant
(
    d Map(String, DateTime('UTC')),
    u Map(String, UUID),
    e Map(Enum8('a' = 1, 'b' = 2), String),
    INDEX idx_d mapValues(d) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_u mapValues(u) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_e mapKeys(e) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

CREATE TABLE t_bloom_filter_map_constant_plain
(
    d Map(String, DateTime('UTC')),
    u Map(String, UUID),
    e Map(Enum8('a' = 1, 'b' = 2), String)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_bloom_filter_map_constant VALUES
    (map('k', '2020-01-01 00:00:00'), map('k', '00000000-0000-0000-0000-000000000001'), map('a', 'x')),
    (map('k', '2021-01-01 00:00:00'), map('k', '00000000-0000-0000-0000-000000000002'), map('b', 'y'));

INSERT INTO t_bloom_filter_map_constant_plain SELECT * FROM t_bloom_filter_map_constant;

SELECT 'dateTime', count() FROM t_bloom_filter_map_constant WHERE d['k'] = '2020-01-01 00:00:00' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'dateTime', count() FROM t_bloom_filter_map_constant WHERE d['k'] = '2020-01-01 00:00:00' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'dateTime', count() FROM t_bloom_filter_map_constant_plain WHERE d['k'] = '2020-01-01 00:00:00';

SELECT 'uuid', count() FROM t_bloom_filter_map_constant WHERE u['k'] = '00000000-0000-0000-0000-000000000001' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'uuid', count() FROM t_bloom_filter_map_constant WHERE u['k'] = '00000000-0000-0000-0000-000000000001' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'uuid', count() FROM t_bloom_filter_map_constant_plain WHERE u['k'] = '00000000-0000-0000-0000-000000000001';

-- An `Enum` map *key*, which the `arrayElement` form leaves a `String` literal. `IN` goes through
-- `traverseTreeIn`, which hashes the key the same way and needs the same conversion.
SELECT 'enumKey', count() FROM t_bloom_filter_map_constant WHERE e['a'] = 'x' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'enumKey', count() FROM t_bloom_filter_map_constant WHERE e['a'] = 'x' SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'enumKey', count() FROM t_bloom_filter_map_constant_plain WHERE e['a'] = 'x';

SELECT 'enumKeyIn', count() FROM t_bloom_filter_map_constant WHERE e['a'] IN ('x') SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'enumKeyIn', count() FROM t_bloom_filter_map_constant WHERE e['a'] IN ('x') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'enumKeyIn', count() FROM t_bloom_filter_map_constant_plain WHERE e['a'] IN ('x');

-- A constant that does convert is still hashed and the index still prunes, so declining the atom is not
-- what makes the answers above right.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_bloom_filter_map_constant WHERE d['k'] = '2020-01-01 00:00:00'
    SETTINGS optimize_functions_to_subcolumns = 1, use_skip_indexes_on_data_read = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_bloom_filter_map_constant WHERE e['a'] = 'x'
    SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes_on_data_read = 0
) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE t_bloom_filter_map_constant;
DROP TABLE t_bloom_filter_map_constant_plain;
