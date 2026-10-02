-- A LowCardinality floating point -0.0 keeps its sign when written to a MergeTree part, so a sorting key
-- that distinguishes it from 0.0 matches the stored data.
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_neg_zero;
CREATE TABLE t_lc_neg_zero (x LowCardinality(Float64)) ENGINE = MergeTree ORDER BY murmurHash3_64(x);
INSERT INTO t_lc_neg_zero VALUES (-0.0), (4.0);
SELECT hex(reinterpretAsUInt64(toFloat64(x))) FROM t_lc_neg_zero ORDER BY x;
SELECT count() FROM t_lc_neg_zero WHERE x = -0.0;
OPTIMIZE TABLE t_lc_neg_zero FINAL;
SELECT hex(reinterpretAsUInt64(toFloat64(x))) FROM t_lc_neg_zero ORDER BY x;
SELECT count() FROM t_lc_neg_zero WHERE x = -0.0;
DROP TABLE t_lc_neg_zero;

DROP TABLE IF EXISTS t_lc_neg_zero_nullable;
CREATE TABLE t_lc_neg_zero_nullable (x LowCardinality(Nullable(Float64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_neg_zero_nullable VALUES (-0.0), (NULL), (0.0);
SELECT hex(reinterpretAsUInt64(toFloat64(x))) FROM t_lc_neg_zero_nullable WHERE x IS NOT NULL ORDER BY hex(reinterpretAsUInt64(toFloat64(x)));
DROP TABLE t_lc_neg_zero_nullable;

DROP TABLE IF EXISTS t_lc_neg_zero_f32;
CREATE TABLE t_lc_neg_zero_f32 (x LowCardinality(Float32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_neg_zero_f32 VALUES (-0.0);
SELECT hex(reinterpretAsUInt32(toFloat32(x))) FROM t_lc_neg_zero_f32;
DROP TABLE t_lc_neg_zero_f32;

DROP TABLE IF EXISTS t_lc_neg_zero_map;
CREATE TABLE t_lc_neg_zero_map (c0 Map(LowCardinality(Float64), UInt8)) ENGINE = MergeTree ORDER BY murmurHash3_64(c0);
INSERT INTO t_lc_neg_zero_map VALUES (map(-0.0, 1)), (map(4.0, 2));
SELECT arrayMap(k -> hex(reinterpretAsUInt64(toFloat64(k))), mapKeys(c0)) FROM t_lc_neg_zero_map ORDER BY mapValues(c0);
DROP TABLE t_lc_neg_zero_map;
