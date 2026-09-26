-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 19-21 and 26-29 of the series started in 04165_skip_index_stale_type_after_alter: the
-- carriers of SUBCOLUMN indexes, and the LowCardinality framing. One test exceeded the flaky-check
-- runtime limit under sanitizers, so the series is split, keeping the original case numbering.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 19. the carrier of a SUBCOLUMN index is its parent, whose type the cache also erases';
-- p.x is not a top-level column of the part, so the part-side type must be derived from the part's
-- OWN Tuple type. DataTypeTuple::equals() recurses into DataTypeNumber<UInt8>::equals(), a bare
-- typeid test, so Tuple(x UInt8) and Tuple(x Bool) share one interned entry while toString(p.x)
-- changes '0'/'1' to 'false'/'true'. ATTACH PARTITION FROM makes the collision deterministic the
-- same way case 15 (in 04870_skip_index_stale_type_metadata_only_alter) does.
DROP TABLE IF EXISTS t_sub_src;
CREATE TABLE t_sub_src (k UInt64, p Tuple(x UInt8), INDEX idx toString(p.x) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_sub_src SELECT number, tuple(number % 2) FROM numbers(64);
SYSTEM STOP MERGES t_sub_src;
DROP TABLE IF EXISTS t_sub_dst;
CREATE TABLE t_sub_dst (k UInt64, p Tuple(x Bool), INDEX idx toString(p.x) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_sub_dst SELECT 1000 + number, tuple(number % 2) FROM numbers(4);
SYSTEM STOP MERGES t_sub_dst;
ALTER TABLE t_sub_dst ATTACH PARTITION 0 FROM t_sub_src SETTINGS alter_sync = 2;
SELECT count() FROM t_sub_dst WHERE toString(p.x) = 'true' AND k < 1000 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_sub_dst WHERE toString(p.x) = 'true' AND k < 1000 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;

SELECT '-- 20. over-fire control: an UNCHANGED subcolumn index must keep pruning';
-- The refusal is about a parent type that CHANGED, not about the requirement being a subcolumn: a
-- fix that refused every subcolumn index, or that compared a part-side parent against a
-- metadata-side subcolumn, would lose pruning here. Both the bare and the expression shape.
DROP TABLE IF EXISTS t_keep_sub_src;
CREATE TABLE t_keep_sub_src (k UInt64, p Tuple(x UInt8), INDEX idx p.x TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_sub_src SELECT number, tuple(intDiv(number, 16)) FROM numbers(64);
SYSTEM STOP MERGES t_keep_sub_src;
DROP TABLE IF EXISTS t_keep_sub_dst;
CREATE TABLE t_keep_sub_dst (k UInt64, p Tuple(x UInt8), INDEX idx p.x TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_sub_dst SELECT 1000 + number, tuple(9) FROM numbers(4);
SYSTEM STOP MERGES t_keep_sub_dst;
ALTER TABLE t_keep_sub_dst ATTACH PARTITION 0 FROM t_keep_sub_src SETTINGS alter_sync = 2;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_sub_dst WHERE p.x = 0) WHERE explain ILIKE '%Granules: 4/17%';
SELECT count() FROM t_keep_sub_dst WHERE p.x = 0;
DROP TABLE IF EXISTS t_keep_subexpr_src;
CREATE TABLE t_keep_subexpr_src (k UInt64, p Tuple(x UInt8), INDEX idx toString(p.x) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_subexpr_src SELECT number, tuple(intDiv(number, 16)) FROM numbers(64);
SYSTEM STOP MERGES t_keep_subexpr_src;
DROP TABLE IF EXISTS t_keep_subexpr_dst;
CREATE TABLE t_keep_subexpr_dst (k UInt64, p Tuple(x UInt8), INDEX idx toString(p.x) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_subexpr_dst SELECT 1000 + number, tuple(9) FROM numbers(4);
SYSTEM STOP MERGES t_keep_subexpr_dst;
ALTER TABLE t_keep_subexpr_dst ATTACH PARTITION 0 FROM t_keep_subexpr_src SETTINGS alter_sync = 2;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_subexpr_dst WHERE toString(p.x) = '0') WHERE explain ILIKE '%Granules: 4/17%';
SELECT count() FROM t_keep_subexpr_dst WHERE toString(p.x) = '0';

SELECT '-- 21. over-fire control: a subcolumn the part list cannot describe keeps pruning after a reload';
-- `vec.quantized` comes from a custom SERIALIZATION the Quantized codec attaches to the metadata-side
-- type, not from the declared type, so columns.txt cannot represent it (IMergeTreeReader.cpp) and the
-- part's own parent type offers no such subcolumn once the part is reloaded from disk. That is an
-- unrepresentable-in-columns.txt fact, not a type difference, so pruning must survive the reload -
-- a refusal here would answer differently before and after a restart.
-- Independently of that, no stale-type shape is constructible for this carrier at all:
-- MergeTreeData::checkAlterIsPossible rejects adding, removing or changing a Quantized(...) codec and
-- restating the type of a Quantized-coded column ("The Quantized(...) codec is immutable via ALTER").
SET enable_quantized_codec = 1;
DROP TABLE IF EXISTS t_keep_quantized;
CREATE TABLE t_keep_quantized (k UInt64, vec Array(Float32) CODEC(Quantized('int8', 8)),
    INDEX idx vec.quantized TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_quantized SELECT number, arrayMap(j -> toFloat32(number + j), range(8)) FROM numbers(64);
SYSTEM STOP MERGES t_keep_quantized;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_quantized WHERE vec.quantized = 'xxxxxxxxxxxx') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() FROM t_keep_quantized WHERE vec.quantized = 'xxxxxxxxxxxx';
DETACH TABLE t_keep_quantized SYNC;
ATTACH TABLE t_keep_quantized;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_quantized WHERE vec.quantized = 'xxxxxxxxxxxx') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() FROM t_keep_quantized WHERE vec.quantized = 'xxxxxxxxxxxx';
SELECT count() FROM t_keep_quantized WHERE vec.quantized = 'xxxxxxxxxxxx' SETTINGS use_skip_indexes = 0;
-- The same refusal is reachable through the other branch: no IDataType::equals() compares
-- custom_serialization, so a part whose own list is uncustomized shares the interned
-- ColumnsDescription of a customized one and tryGetColumn() then SUCCEEDS, while the part's own
-- parent still offers no subcolumn. ATTACH PARTITION FROM makes that collision deterministic the
-- same way case 15 (in 04870_skip_index_stale_type_metadata_only_alter) does. Both branches must
-- answer alike, or pruning would depend on load order.
DROP TABLE IF EXISTS t_keep_quant_src;
CREATE TABLE t_keep_quant_src (k UInt64, vec Array(Float32) CODEC(Quantized('int8', 8)),
    INDEX idx vec.quantized TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_quant_src SELECT number, arrayMap(j -> toFloat32(number + j), range(8)) FROM numbers(64);
SYSTEM STOP MERGES t_keep_quant_src;
DROP TABLE IF EXISTS t_keep_quant_dst;
CREATE TABLE t_keep_quant_dst (k UInt64, vec Array(Float32) CODEC(Quantized('int8', 8)),
    INDEX idx vec.quantized TYPE minmax GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_quant_dst SELECT 1000 + number, arrayMap(j -> toFloat32(number + j), range(8)) FROM numbers(4);
SYSTEM STOP MERGES t_keep_quant_dst;
ALTER TABLE t_keep_quant_dst ATTACH PARTITION 0 FROM t_keep_quant_src SETTINGS alter_sync = 2;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_quant_dst WHERE vec.quantized = 'xxxxxxxxxxxx') WHERE explain ILIKE '%Granules: 0/17%';
SELECT count() FROM t_keep_quant_dst WHERE vec.quantized = 'xxxxxxxxxxxx';
SELECT count() FROM t_keep_quant_dst WHERE vec.quantized = 'xxxxxxxxxxxx' SETTINGS use_skip_indexes = 0;

-- A dotted PARENT name pins the name split: `a.b`.quantized must resolve as `a.b` + `quantized`,
-- not as the shortest prefix `a` + `b.quantized`, which resolves to no column and loses pruning.
DROP TABLE IF EXISTS t_keep_quant_dotted;
CREATE TABLE t_keep_quant_dotted (k UInt64, `a.b` Array(Float32) CODEC(Quantized('int8', 8)),
    INDEX idx `a.b`.quantized TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_quant_dotted SELECT number, arrayMap(j -> toFloat32(number + j), range(8)) FROM numbers(64);
SYSTEM STOP MERGES t_keep_quant_dotted;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_quant_dotted WHERE `a.b`.quantized = 'xxxxxxxxxxxx') WHERE explain ILIKE '%Granules: 0/16%';
DETACH TABLE t_keep_quant_dotted SYNC;
ATTACH TABLE t_keep_quant_dotted;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_quant_dotted WHERE `a.b`.quantized = 'xxxxxxxxxxxx') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() FROM t_keep_quant_dotted WHERE `a.b`.quantized = 'xxxxxxxxxxxx';
SELECT count() FROM t_keep_quant_dotted WHERE `a.b`.quantized = 'xxxxxxxxxxxx' SETTINGS use_skip_indexes = 0;

-- LowCardinality keeps its own framing (a dictionary plus indexes) whatever the dictionary type is,
-- so a dictionary-side representation-preserving conversion must still prune. Cases 26-27 are the
-- wrapped forms of the allow-list; 28-29 pin that the wrapper itself is not part of the allowance.
SET allow_suspicious_low_cardinality_types = 1;

SELECT '-- 26. over-fire control: LowCardinality(DateTime) -> LowCardinality(UInt32) still prunes';
DROP TABLE IF EXISTS t_keep_lc;
CREATE TABLE t_keep_lc (k UInt64, v LowCardinality(DateTime), INDEX idx v TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_lc SELECT number, toDateTime(1600000000 + intDiv(number, 4) * 3600) FROM numbers(64);
SYSTEM STOP MERGES t_keep_lc;
ALTER TABLE t_keep_lc MODIFY COLUMN v LowCardinality(UInt32);
KILL MUTATION WHERE table = 't_keep_lc' AND database = currentDatabase() FORMAT Null;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_lc WHERE v = 1600003600) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_lc WHERE v = 1600003600;
SELECT count() FROM t_keep_lc WHERE v = 1600003600 SETTINGS use_skip_indexes = 0;

SELECT '-- 27. over-fire control: Array(LowCardinality(Date)) -> Array(LowCardinality(UInt16)) still prunes';
-- Nested inside Array, so the walk has to reach the dictionary through another wrapper.
DROP TABLE IF EXISTS t_keep_lc_arr;
CREATE TABLE t_keep_lc_arr (k UInt64, v Array(LowCardinality(Date)), INDEX idx v TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_lc_arr SELECT number, [toDate('2020-01-01') + intDiv(number, 4)] FROM numbers(64);
SYSTEM STOP MERGES t_keep_lc_arr;
ALTER TABLE t_keep_lc_arr MODIFY COLUMN v Array(LowCardinality(UInt16));
KILL MUTATION WHERE table = 't_keep_lc_arr' AND database = currentDatabase() FORMAT Null;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_lc_arr WHERE has(v, 18264)) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_lc_arr WHERE has(v, 18264);
SELECT count() FROM t_keep_lc_arr WHERE has(v, 18264) SETTINGS use_skip_indexes = 0;

SELECT '-- 28. dropping the LowCardinality wrapper is a framing change, so it must refuse';
-- Cases 28-29 refuse with or without case 26's branch, since a one-sided wrapper falls through to
-- false either way. What they pin is that the match stays PAIRWISE: the granule holds a dictionary
-- plus indexes, so read as a bare column those bytes are not values at all. Unwrapping one side only
-- reintroduces the PARAMETER_OUT_OF_BOUND that this case's granule assertion then catches.
DROP TABLE IF EXISTS t_stale_lc_unwrap;
CREATE TABLE t_stale_lc_unwrap (k UInt64, v LowCardinality(DateTime), INDEX idx v TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_lc_unwrap SELECT number, toDateTime(1600000000 + intDiv(number, 4) * 3600) FROM numbers(64);
SYSTEM STOP MERGES t_stale_lc_unwrap;
ALTER TABLE t_stale_lc_unwrap MODIFY COLUMN v DateTime;
KILL MUTATION WHERE table = 't_stale_lc_unwrap' AND database = currentDatabase() FORMAT Null;
-- The result alone would also pass if the index were used and merely happened not to misprune this
-- value, so assert the index was refused: no granule is dropped.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stale_lc_unwrap WHERE v = 1600003600) WHERE explain ILIKE '%Granules: 16/16%';
SELECT count() FROM t_stale_lc_unwrap WHERE v = 1600003600;
SELECT count() FROM t_stale_lc_unwrap WHERE v = 1600003600 SETTINGS use_skip_indexes = 0;

SELECT '-- 29. adding the LowCardinality wrapper must refuse for the same reason';
DROP TABLE IF EXISTS t_stale_lc_wrap;
CREATE TABLE t_stale_lc_wrap (k UInt64, v DateTime, INDEX idx v TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_lc_wrap SELECT number, toDateTime(1600000000 + intDiv(number, 4) * 3600) FROM numbers(64);
SYSTEM STOP MERGES t_stale_lc_wrap;
ALTER TABLE t_stale_lc_wrap MODIFY COLUMN v LowCardinality(DateTime);
KILL MUTATION WHERE table = 't_stale_lc_wrap' AND database = currentDatabase() FORMAT Null;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_stale_lc_wrap WHERE v = 1600003600) WHERE explain ILIKE '%Granules: 16/16%';
SELECT count() FROM t_stale_lc_wrap WHERE v = 1600003600;
SELECT count() FROM t_stale_lc_wrap WHERE v = 1600003600 SETTINGS use_skip_indexes = 0;

DROP TABLE t_sub_src;
DROP TABLE t_sub_dst;
DROP TABLE t_keep_sub_src;
DROP TABLE t_keep_sub_dst;
DROP TABLE t_keep_subexpr_src;
DROP TABLE t_keep_subexpr_dst;
DROP TABLE t_keep_quantized;
DROP TABLE t_keep_quant_src;
DROP TABLE t_keep_quant_dst;
DROP TABLE t_keep_quant_dotted;
DROP TABLE t_keep_lc;
DROP TABLE t_keep_lc_arr;
DROP TABLE t_stale_lc_unwrap;
DROP TABLE t_stale_lc_wrap;
