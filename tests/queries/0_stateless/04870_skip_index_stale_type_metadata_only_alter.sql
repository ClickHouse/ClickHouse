-- Tags: no-fasttest, no-random-merge-tree-settings, no-parallel-replicas
-- no-fasttest: the JSON control case needs the JSON type.
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 13-18 of the series started in 04165_skip_index_stale_type_after_alter: metadata-only ALTERs
-- where no mutation is ever created and only an attribute of the type changes. One test exceeded the
-- flaky-check runtime limit under sanitizers, so the series is split, keeping the original case
-- numbering.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 13. expression index over a timezone-dependent expression, timezone-only MODIFY COLUMN';
-- Both timezones are pinned in the DDL: session_timezone is randomized by the test runner, so a
-- fixture relying on the server default is not reproducible.
DROP TABLE IF EXISTS t_stale_tz;
CREATE TABLE t_stale_tz (k UInt64, dt DateTime('UTC'), INDEX idx toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_tz SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_stale_tz;
ALTER TABLE t_stale_tz MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT count() FROM system.mutations WHERE table = 't_stale_tz' AND database = currentDatabase();
-- Hour 9 exists only in the NEW timezone, so a granule computed in the old one prunes it away. The
-- query condition cache is pinned off because it is keyed on the condition rather than on the index,
-- so a verdict cached by one of these two statements would answer the other one too.
SELECT count() FROM t_stale_tz WHERE toHour(dt) = 9 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_stale_tz WHERE toHour(dt) = 9 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;
-- Nullable and DateTime64 reach the same DateTime attribution through a wrapper and a sibling type.
DROP TABLE IF EXISTS t_stale_tz_nullable;
CREATE TABLE t_stale_tz_nullable (k UInt64, dt Nullable(DateTime('UTC')), INDEX idx toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_tz_nullable SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_stale_tz_nullable;
ALTER TABLE t_stale_tz_nullable MODIFY COLUMN dt Nullable(DateTime('Asia/Tokyo'));
SELECT count() FROM t_stale_tz_nullable WHERE toHour(dt) = 9 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_stale_tz_nullable WHERE toHour(dt) = 9 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;
DROP TABLE IF EXISTS t_stale_tz64;
CREATE TABLE t_stale_tz64 (k UInt64, dt DateTime64(3, 'UTC'), INDEX idx toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_tz64 SELECT number, toDateTime64('2020-01-01 00:00:00', 3, 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_stale_tz64;
ALTER TABLE t_stale_tz64 MODIFY COLUMN dt DateTime64(3, 'Asia/Tokyo');
SELECT count() FROM t_stale_tz64 WHERE toHour(dt) = 9 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_stale_tz64 WHERE toHour(dt) = 9 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;

SELECT '-- 14. control: a simple single-column index keeps pruning across the same timezone ALTER';
-- A set/minmax granule over the bare column holds the raw epoch value, which no timezone changes, so
-- refusing this very common ALTER would be a pruning regression.
DROP TABLE IF EXISTS t_keep_tz;
CREATE TABLE t_keep_tz (k UInt64, dt DateTime('UTC'), INDEX idx dt TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_tz SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_keep_tz;
ALTER TABLE t_keep_tz MODIFY COLUMN dt DateTime('Asia/Tokyo');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_tz WHERE dt = toDateTime('2020-01-01 04:00:00', 'UTC')) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_tz WHERE dt = toDateTime('2020-01-01 04:00:00', 'UTC');
DROP TABLE IF EXISTS t_keep_tz_nullable;
CREATE TABLE t_keep_tz_nullable (k UInt64, dt Nullable(DateTime('UTC')), INDEX idx dt TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_tz_nullable SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_keep_tz_nullable;
ALTER TABLE t_keep_tz_nullable MODIFY COLUMN dt Nullable(DateTime('Asia/Tokyo'));
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_tz_nullable WHERE dt = toDateTime('2020-01-01 04:00:00', 'UTC')) WHERE explain ILIKE '%Granules: 1/16%';
-- An expression index whose column keeps its timezone must also keep pruning: the refusal is about a
-- timezone that CHANGED, not about a DateTime column being present.
DROP TABLE IF EXISTS t_keep_tz_expr;
CREATE TABLE t_keep_tz_expr (k UInt64, dt DateTime('UTC'), INDEX idx toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_tz_expr SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_keep_tz_expr;
ALTER TABLE t_keep_tz_expr MODIFY COLUMN dt DateTime('UTC');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_tz_expr WHERE toHour(dt) = 5) WHERE explain ILIKE '%Granules: 3/16%';

SELECT '-- 15. the part-side type must come from the part, not from the interned description';
-- IMergeTreeDataPart::tryGetColumn() answers from a storage-wide ColumnsDescription interning cache
-- whose key equality is IDataType::equals(), which drops the timezone. So a part written under one
-- timezone can report another part's, and part loading is concurrent and shuffled. ATTACH PARTITION
-- FROM makes that collision deterministic: the destination interns its Asia/Tokyo column list first,
-- then the UTC-written part arrives and hits that entry.
DROP TABLE IF EXISTS t_cache_src;
CREATE TABLE t_cache_src (k UInt64, dt DateTime('UTC'), INDEX idx toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_cache_src SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600 FROM numbers(64);
SYSTEM STOP MERGES t_cache_src;
DROP TABLE IF EXISTS t_cache_dst;
CREATE TABLE t_cache_dst (k UInt64, dt DateTime('Asia/Tokyo'), INDEX idx toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree PARTITION BY intDiv(k, 1000) ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_cache_dst SELECT 1000 + number, toDateTime('2020-01-01 00:00:00', 'Asia/Tokyo') + number * 3600 FROM numbers(4);
SYSTEM STOP MERGES t_cache_dst;
ALTER TABLE t_cache_dst ATTACH PARTITION 0 FROM t_cache_src SETTINGS alter_sync = 2;
SELECT count() FROM t_cache_dst WHERE toHour(dt) = 9 AND k < 1000 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_cache_dst WHERE toHour(dt) = 9 AND k < 1000 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;

SELECT '-- 16. timezone nested inside an AggregateFunction argument';
-- A bare AggregateFunction column cannot be indexed (minmaxIndexValidator inspects the expression's
-- result type), but finalizeAggregation() of one can, and its argument timezone is dropped by
-- DataTypeAggregateFunction::equals(), which compares argument_types with equals() in turn.
DROP TABLE IF EXISTS t_stale_agg;
CREATE TABLE t_stale_agg (k UInt64, v AggregateFunction(max, DateTime('UTC')),
    INDEX idx toHour(finalizeAggregation(v)) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_agg SELECT number,
    initializeAggregation('maxState', toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600) FROM numbers(64);
SYSTEM STOP MERGES t_stale_agg;
ALTER TABLE t_stale_agg MODIFY COLUMN v AggregateFunction(max, DateTime('Asia/Tokyo'));
SELECT count() FROM system.mutations WHERE table = 't_stale_agg' AND database = currentDatabase();
SELECT count() FROM t_stale_agg WHERE toHour(finalizeAggregation(v)) = 9 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_stale_agg WHERE toHour(finalizeAggregation(v)) = 9 SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;
-- Over-fire control: the same fixture with the timezone unchanged must keep pruning.
DROP TABLE IF EXISTS t_keep_agg;
CREATE TABLE t_keep_agg (k UInt64, v AggregateFunction(max, DateTime('UTC')),
    INDEX idx toHour(finalizeAggregation(v)) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_agg SELECT number,
    initializeAggregation('maxState', toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600) FROM numbers(64);
SYSTEM STOP MERGES t_keep_agg;
ALTER TABLE t_keep_agg MODIFY COLUMN v AggregateFunction(max, DateTime('UTC'));
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_agg WHERE toHour(finalizeAggregation(v)) = 5) WHERE explain ILIKE '%Granules: 3/16%';

SELECT '-- 17. custom type name over the same underlying type: UInt8 -> Bool';
-- Bool is a custom name plus a custom serialization over DataTypeNumber<UInt8>, whose equals()
-- compares only typeid, so this ALTER is metadata-only while toString(v) changes '1' to 'true'.
DROP TABLE IF EXISTS t_stale_bool;
CREATE TABLE t_stale_bool (k UInt64, v UInt8, INDEX idx toString(v) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_bool SELECT number, number % 2 FROM numbers(64);
SYSTEM STOP MERGES t_stale_bool;
ALTER TABLE t_stale_bool MODIFY COLUMN v Bool;
SELECT count() FROM system.mutations WHERE table = 't_stale_bool' AND database = currentDatabase();
SELECT count() FROM t_stale_bool WHERE toString(v) = 'true' SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_stale_bool WHERE toString(v) = 'true' SETTINGS use_skip_indexes = 0, use_query_condition_cache = 0;
-- Over-fire control: a granule over the bare column holds the raw byte, which the custom name does
-- not alter, so a simple single-column index must keep pruning. Same asymmetry as case 14.
DROP TABLE IF EXISTS t_keep_bool;
CREATE TABLE t_keep_bool (k UInt64, v UInt8, INDEX idx v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_bool SELECT number, intDiv(number, 16) FROM numbers(64);
SYSTEM STOP MERGES t_keep_bool;
ALTER TABLE t_keep_bool MODIFY COLUMN v Bool;
-- optimize_trivial_count_query: a bare count() over this predicate is answerable from sparsity
-- statistics alone, which replaces the whole read and leaves no skip-index node to assert on.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_bool WHERE v = 0 SETTINGS optimize_trivial_count_query = 0) WHERE explain ILIKE '%Granules: 4/16%';
SELECT count() FROM t_keep_bool WHERE v = 0;

SELECT '-- 18. control: an UNCHANGED JSON column with a typed DateTime path keeps pruning';
SET enable_json_type = 1;
-- No ALTER at all here. A fail-closed fallback that merely asks "is a DateTime reachable from either
-- side" answers yes for two byte-identical JSON types, and every non-trivial index over such a
-- column would then lose pruning on every part forever.
DROP TABLE IF EXISTS t_keep_json_tz;
CREATE TABLE t_keep_json_tz (k UInt64, j JSON(a DateTime('UTC')), INDEX idx toString(j) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_json_tz SELECT number, toJSONString(map('a', toString(toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600))) FROM numbers(64);
SYSTEM STOP MERGES t_keep_json_tz;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_json_tz WHERE toString(j) = 'x') WHERE explain ILIKE '%Granules: 0/16%';
-- The same shape whose typed path is not a DateTime pruned correctly before this fix too, so it
-- separates "the JSON pair is compared" from "a DateTime is merely reachable".
DROP TABLE IF EXISTS t_keep_json_int;
CREATE TABLE t_keep_json_int (k UInt64, j JSON(a UInt64), INDEX idx toString(j) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_json_int SELECT number, toJSONString(map('a', toString(number))) FROM numbers(64);
SYSTEM STOP MERGES t_keep_json_int;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_json_int WHERE toString(j) = 'x') WHERE explain ILIKE '%Granules: 0/16%';

DROP TABLE t_stale_tz;
DROP TABLE t_stale_tz_nullable;
DROP TABLE t_stale_tz64;
DROP TABLE t_keep_tz;
DROP TABLE t_keep_tz_nullable;
DROP TABLE t_keep_tz_expr;
DROP TABLE t_cache_src;
DROP TABLE t_cache_dst;
DROP TABLE t_stale_agg;
DROP TABLE t_keep_agg;
DROP TABLE t_stale_bool;
DROP TABLE t_keep_bool;
DROP TABLE t_keep_json_tz;
DROP TABLE t_keep_json_int;
