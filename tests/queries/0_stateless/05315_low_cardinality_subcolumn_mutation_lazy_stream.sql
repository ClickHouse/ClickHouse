-- A mutation reads a column together with its subcolumn while both share a LowCardinality
-- dictionary stream that the Wide part reader opens only after reading the prefixes.

DROP TABLE IF EXISTS t_map_lc;
CREATE TABLE t_map_lc (id UInt64, m Map(LowCardinality(String), LowCardinality(String)), n UInt64 DEFAULT 0)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_map_lc (id, m) SELECT number, map(concat('k', toString(number % 10)), concat('v', toString(number % 10)), 'common', 'c') FROM numbers(10000);
ALTER TABLE t_map_lc UPDATE n = length(m.keys) + length(toString(m)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT sum(n) FROM t_map_lc;
ALTER TABLE t_map_lc UPDATE m = mapConcat(m, map('x', 'y')) WHERE has(m.keys, 'k1') SETTINGS mutations_sync = 2;
SELECT countIf(has(m.keys, 'x')), countIf(m.keys != mapKeys(m) OR m.values != mapValues(m)) FROM t_map_lc;
DROP TABLE t_map_lc;

DROP TABLE IF EXISTS t_map_lc_values;
CREATE TABLE t_map_lc_values (id UInt64, m Map(String, LowCardinality(String)), n UInt64 DEFAULT 0)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_map_lc_values (id, m) SELECT number, map(concat('k', toString(number % 10)), concat('v', toString(number % 10)), 'common', 'c') FROM numbers(10000);
ALTER TABLE t_map_lc_values UPDATE n = length(m.values) + length(toString(m)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT sum(n) FROM t_map_lc_values;
DROP TABLE t_map_lc_values;

DROP TABLE IF EXISTS t_dynamic_lc;
CREATE TABLE t_dynamic_lc (id UInt64, d Dynamic, n UInt64 DEFAULT 0)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dynamic_lc (id, d) SELECT number, toLowCardinality(toString(number % 10)) FROM numbers(10000);
ALTER TABLE t_dynamic_lc UPDATE n = length(d.`LowCardinality(String)`) + length(toString(d)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT sum(n), countIf(d.`LowCardinality(String)` != toString(d)) FROM t_dynamic_lc;
DROP TABLE t_dynamic_lc;
