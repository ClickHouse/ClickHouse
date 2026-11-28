-- `...Map` Nested columns whose element type has `LowCardinality` inside a compound type.

CREATE TABLE sum_lc_value (k UInt64, statsMap Nested(key LowCardinality(String), hits UInt64, labels Array(LowCardinality(String)))) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO sum_lc_value VALUES (1, ['a'], [1], [['x']]);
INSERT INTO sum_lc_value VALUES (1, ['a'], [1], [['x']]);
OPTIMIZE TABLE sum_lc_value FINAL;
SELECT * FROM sum_lc_value;

CREATE TABLE sum_lc_key (k UInt64, fooMap Nested(id Array(LowCardinality(String)), val UInt64)) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO sum_lc_key VALUES (1, [['a']], [1]);
INSERT INTO sum_lc_key VALUES (1, [['a']], [1]);
OPTIMIZE TABLE sum_lc_key FINAL;
SELECT * FROM sum_lc_key;

CREATE TABLE sum_lc_map (k UInt64, attrsMap Nested(id UInt64, attrs Map(LowCardinality(String), UInt64))) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO sum_lc_map VALUES (1, [1], [map('a', 1)]);
INSERT INTO sum_lc_map VALUES (1, [1], [map('a', 1)]);
OPTIMIZE TABLE sum_lc_map FINAL;
SELECT * FROM sum_lc_map;
