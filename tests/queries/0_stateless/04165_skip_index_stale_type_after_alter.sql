-- Tags: no-fasttest, no-random-merge-tree-settings, no-parallel-replicas
-- no-fasttest: the JSON case needs the JSON type.
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block), and use_skip_indexes_on_data_read is not supported with parallel replicas.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 1. killed mutation, String -> Nullable(UInt64)';
DROP TABLE IF EXISTS t_stale_nullable;
CREATE TABLE t_stale_nullable (k UInt64, value String, INDEX idx value TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_nullable SELECT number, toString(intDiv(number, 4)) FROM numbers(64);
SYSTEM STOP MERGES t_stale_nullable;
ALTER TABLE t_stale_nullable MODIFY COLUMN value Nullable(UInt64);
KILL MUTATION WHERE table = 't_stale_nullable' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_stale_nullable WHERE value = 3;

SELECT '-- 2. killed mutation, String -> UInt64 (not Nullable-specific)';
DROP TABLE IF EXISTS t_stale_plain;
CREATE TABLE t_stale_plain (k UInt64, value String, INDEX idx value TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_plain SELECT number, toString(number * 3) FROM numbers(64);
SYSTEM STOP MERGES t_stale_plain;
ALTER TABLE t_stale_plain MODIFY COLUMN value UInt64;
KILL MUTATION WHERE table = 't_stale_plain' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_stale_plain WHERE value = 150;

SELECT '-- 3. JSON type hint, no mutation is ever created';
SET allow_experimental_json_lazy_type_hints = 1, enable_json_type = 1;
DROP TABLE IF EXISTS t_stale_json;
CREATE TABLE t_stale_json (k UInt64, j JSON, INDEX idx j.a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_json SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
SYSTEM STOP MERGES t_stale_json;
ALTER TABLE t_stale_json MODIFY COLUMN j JSON(a UInt64);
SELECT count() FROM system.mutations WHERE table = 't_stale_json' AND database = currentDatabase();
SELECT count() FROM t_stale_json WHERE j.a = 150;

SELECT '-- 4. expression index, representation-preserving column conversion';
DROP TABLE IF EXISTS t_stale_expr;
CREATE TABLE t_stale_expr (k UInt64, d Date, INDEX idx (d + 1) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_expr SELECT number, toDate('2020-01-01') + number FROM numbers(64);
SYSTEM STOP MERGES t_stale_expr;
ALTER TABLE t_stale_expr MODIFY COLUMN d UInt16;
SELECT count() FROM system.mutations WHERE table = 't_stale_expr' AND database = currentDatabase();
SELECT count() FROM t_stale_expr WHERE (d + 1) = 18264;

SELECT '-- 5. killed mutation, Int8 -> Enum8: must not prune away a value the read rejects';
DROP TABLE IF EXISTS t_stale_enum;
CREATE TABLE t_stale_enum (k UInt64, v Int8, INDEX idx v TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_enum SELECT number, 3 FROM numbers(8);
SYSTEM STOP MERGES t_stale_enum;
ALTER TABLE t_stale_enum MODIFY COLUMN v Enum8('a' = 1, 'b' = 2);
KILL MUTATION WHERE table = 't_stale_enum' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_stale_enum WHERE v = 'a'; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }

SELECT '-- 6. killed mutation, minmax index read without a canUseIndex guard';
DROP TABLE IF EXISTS t_stale_minmax;
CREATE TABLE t_stale_minmax (k UInt64, value String, INDEX idx value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_minmax SELECT number, toString(number * 3) FROM numbers(64);
SYSTEM STOP MERGES t_stale_minmax;
ALTER TABLE t_stale_minmax MODIFY COLUMN value UInt64;
KILL MUTATION WHERE table = 't_stale_minmax' AND database = currentDatabase() FORMAT Null;
-- max_rows_to_read must be neutralized per statement wherever use_skip_indexes_on_data_read = 1
-- matters: supportsSkipIndexesOnDataRead() refuses the read-time path when read_overflow_mode is
-- 'throw' and max_rows_to_read is set, which the stateless test profile always does, so without
-- this the read-time case is vacuous.
SELECT count() FROM t_stale_minmax WHERE value = 150
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;
-- The top-k settings are randomized by the test runner, so pin them per statement: without them
-- these lines can silently run with the top-k optimization off and assert nothing about the index.
-- WHERE plus ORDER BY/LIMIT together is what reaches the read-time pool
-- (MergeTreeIndexReadResultPool); analysis-time top-k requires the WHERE clause to be absent.
SELECT k FROM t_stale_minmax WHERE value = 150 ORDER BY value LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         use_skip_indexes_on_data_read = 1, query_plan_max_limit_for_top_k_optimization = 100000,
         max_rows_to_read = 0;
SELECT k FROM t_stale_minmax ORDER BY value LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         query_plan_max_limit_for_top_k_optimization = 100000;

SELECT '-- 7. control: a representation-preserving conversion keeps pruning';
DROP TABLE IF EXISTS t_keep_date;
CREATE TABLE t_keep_date (k UInt64, d Date, INDEX idx d TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_date SELECT number, toDate('2020-01-01') + number FROM numbers(64);
SYSTEM STOP MERGES t_keep_date;
ALTER TABLE t_keep_date MODIFY COLUMN d UInt16;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_date WHERE d = 18262) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_date WHERE d = 18262;

SELECT '-- 8. control: extending an enum keeps pruning';
DROP TABLE IF EXISTS t_keep_enum;
CREATE TABLE t_keep_enum (k UInt64, e Enum8('a' = 1, 'b' = 2), INDEX idx e TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_enum SELECT number, if(number % 2 = 0, 'a', 'b') FROM numbers(64);
SYSTEM STOP MERGES t_keep_enum;
ALTER TABLE t_keep_enum MODIFY COLUMN e Enum8('a' = 1, 'b' = 2, 'c' = 3);
SELECT count() FROM t_keep_enum WHERE e = 'a';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_enum WHERE e = 'c') WHERE explain ILIKE '%Granules: 0/16%';

SELECT '-- 9. control: an unaltered table keeps pruning, including the top-k read';
DROP TABLE IF EXISTS t_keep_plain;
CREATE TABLE t_keep_plain (k UInt64, value UInt64, INDEX idx value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_plain SELECT number, number * 3 FROM numbers(64);
SYSTEM STOP MERGES t_keep_plain;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_plain WHERE value = 150) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_plain WHERE value = 150;
-- Prove the top-k paths still PRUNE here, not merely that the answer is right: the analysis-time
-- top-k reports its own "Filter TopK Granules" step, and the read-time pool is exercised by the
-- WHERE plus ORDER BY/LIMIT line below.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_keep_plain ORDER BY value DESC LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
             query_plan_max_limit_for_top_k_optimization = 100000)
WHERE explain ILIKE '%Filter TopK Granules%';
SELECT k FROM t_keep_plain ORDER BY value DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         query_plan_max_limit_for_top_k_optimization = 100000;
SELECT k FROM t_keep_plain WHERE value = 150 ORDER BY value LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         use_skip_indexes_on_data_read = 1, query_plan_max_limit_for_top_k_optimization = 100000,
         max_rows_to_read = 0;
-- Assert the read-time pool actually RAN, not merely that the answer is right: when it takes over,
-- index analysis stops filtering and reports every granule, so the summary line reads "Granules: 16".
-- Losing the pool (an unpinned max_rows_to_read, or a future guard) flips this to 1 and reddens here
-- instead of silently degrading the three read-time statements above to result-only checks.
SELECT count() > 0 FROM (EXPLAIN ANALYZE indexes = 1
    SELECT k FROM t_keep_plain WHERE value = 150 ORDER BY value LIMIT 1
    SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
             use_skip_indexes_on_data_read = 1, query_plan_max_limit_for_top_k_optimization = 100000,
             max_rows_to_read = 0, use_query_condition_cache = 0)
-- The ' | ' delimiter is the pretty form, which EXPLAIN ANALYZE enables by default.
WHERE explain ILIKE '%Parts: 1 | Granules: 16%';

-- Known gap, deliberately out of scope: reusing an index NAME after a killed DROP INDEX leaves the
-- old index files in the part while the name now means a different column. Both columns share a
-- type, so no type comparison can detect it and the stale granules still prune. Asserting the
-- current (wrong) answer keeps the gap visible; a fix for index identity staleness flips this line.
SELECT '-- 10. known gap: index name reuse after a killed DROP INDEX';
DROP TABLE IF EXISTS t_name_reuse;
CREATE TABLE t_name_reuse (k UInt64, v1 String, v2 String, INDEX idx v1 TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_name_reuse SELECT number, toString(number * 1000), toString(number) FROM numbers(64);
SYSTEM STOP MERGES t_name_reuse;
ALTER TABLE t_name_reuse DROP INDEX idx;
KILL MUTATION WHERE table = 't_name_reuse' AND database = currentDatabase() FORMAT Null;
ALTER TABLE t_name_reuse ADD INDEX idx v2 TYPE set(100) GRANULARITY 1;
SELECT count() FROM t_name_reuse WHERE v2 = '7';
SELECT count() FROM t_name_reuse WHERE v2 = '7' SETTINGS use_skip_indexes = 0;

SELECT '-- 11. killed mutation on a column the part carries an index for but no bytes of';
DROP TABLE IF EXISTS t_absent_col;
CREATE TABLE t_absent_col (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_col SELECT number, toString(number) FROM numbers(64);
-- A DEFAULT column added to a wide part is metadata-only, so MATERIALIZE INDEX writes the index
-- files without adding the column to the part: the part records no type to compare against.
ALTER TABLE t_absent_col ADD COLUMN c String DEFAULT toString(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_col ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_col MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_col' AND active AND column = 'c';
SYSTEM STOP MERGES t_absent_col;
ALTER TABLE t_absent_col MODIFY COLUMN c Nullable(UInt64);
KILL MUTATION WHERE table = 't_absent_col' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_col WHERE c = 150;

SELECT '-- 12. control: an absent column costs no pruning when the part has no index files';
DROP TABLE IF EXISTS t_pre_add_index;
CREATE TABLE t_pre_add_index (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_pre_add_index SELECT number, number * 3 FROM numbers(64);
SYSTEM STOP MERGES t_pre_add_index;
ALTER TABLE t_pre_add_index ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
SELECT count() FROM t_pre_add_index WHERE c = 150;
DROP TABLE IF EXISTS t_materialized_index;
CREATE TABLE t_materialized_index (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_materialized_index SELECT number, number * 3 FROM numbers(64);
ALTER TABLE t_materialized_index ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_materialized_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_materialized_index;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_materialized_index WHERE c = 150) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_materialized_index WHERE c = 150;

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
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_bool WHERE v = 0) WHERE explain ILIKE '%Granules: 4/16%';
SELECT count() FROM t_keep_bool WHERE v = 0;

SELECT '-- 18. control: an UNCHANGED JSON column with a typed DateTime path keeps pruning';
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

SELECT '-- 19. the carrier of a SUBCOLUMN index is its parent, whose type the cache also erases';
-- p.x is not a top-level column of the part, so the part-side type must be derived from the part's
-- OWN Tuple type. DataTypeTuple::equals() recurses into DataTypeNumber<UInt8>::equals(), a bare
-- typeid test, so Tuple(x UInt8) and Tuple(x Bool) share one interned entry while toString(p.x)
-- changes '0'/'1' to 'false'/'true'. ATTACH PARTITION FROM makes the collision deterministic the
-- same way case 15 does.
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
SET allow_experimental_codecs = 1;
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
-- same way case 15 does. Both branches must answer alike, or pruning would depend on load order.
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

SELECT '-- 22. a SUBCOLUMN whose parent the part does not carry at all must still refuse';
-- Case 11 with a subcolumn requirement: the part holds index files for p.x but no p column, so the
-- part records no type to compare against and the granule holds bytes of the old type. This is the
-- shape that separates "the part cannot express this subcolumn" from "the parent is simply absent" -
-- a guard keyed on parent existence alone would skip the type check here and prune wrongly.
DROP TABLE IF EXISTS t_absent_sub;
CREATE TABLE t_absent_sub (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_sub SELECT number, toString(number) FROM numbers(64);
ALTER TABLE t_absent_sub ADD COLUMN p Tuple(x UInt64) DEFAULT tuple(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_sub ADD INDEX idx p.x TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_sub MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_sub' AND active AND column = 'p';
SYSTEM STOP MERGES t_absent_sub;
ALTER TABLE t_absent_sub MODIFY COLUMN p Tuple(x Nullable(UInt64));
KILL MUTATION WHERE table = 't_absent_sub' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_sub WHERE p.x = 150;
SELECT count() FROM t_absent_sub WHERE p.x = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 23. an absent PHYSICAL column whose name splits onto a custom-serialized neighbour refuses';
-- `b.x` is a real physical column, and its name also splits onto the physical `b`, which carries a
-- custom serialization (Bool) that defines no `x` subcolumn at all. Reading that neighbour as `b.x`'s
-- parent would answer "the part list cannot express this" for a column the part is simply missing,
-- skipping the type check exactly where case 22 requires it. So the escape hatch has to consider the
-- exact name first, and has to require the resolved parent to actually offer the suffix.
DROP TABLE IF EXISTS t_absent_bool_prefix;
CREATE TABLE t_absent_bool_prefix (k UInt64, b Bool) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_bool_prefix SELECT number, number % 2 FROM numbers(64);
ALTER TABLE t_absent_bool_prefix ADD COLUMN `b.x` UInt8 DEFAULT 1 SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_bool_prefix ADD INDEX idx toString(`b.x`) TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_bool_prefix MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_bool_prefix' AND active AND column = 'b.x';
SYSTEM STOP MERGES t_absent_bool_prefix;
ALTER TABLE t_absent_bool_prefix MODIFY COLUMN `b.x` Bool;
KILL MUTATION WHERE table = 't_absent_bool_prefix' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'true';
SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'true' SETTINGS use_skip_indexes = 0;

-- The suffix requirement carries this shape on its own: `a.b`.x is not a physical column at all, and
-- its SHORTEST split resolves to a custom-serialized `a Bool` offering no `b.x`, while the true parent
-- `a.b` is a longer split with no custom serialization. So the walk must reject the short split on the
-- suffix it does not offer, keep looking, and then refuse - a Tuple element is representable in
-- columns.txt, so the part's silence about `a.b` is a genuinely absent column.
DROP TABLE IF EXISTS t_absent_bool_dotted;
CREATE TABLE t_absent_bool_dotted (k UInt64, a Bool) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_bool_dotted SELECT number, number % 2 FROM numbers(64);
ALTER TABLE t_absent_bool_dotted ADD COLUMN `a.b` Tuple(x UInt64) DEFAULT tuple(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_bool_dotted ADD INDEX idx `a.b`.x TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_bool_dotted MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_bool_dotted' AND active AND column = 'a.b';
SYSTEM STOP MERGES t_absent_bool_dotted;
ALTER TABLE t_absent_bool_dotted MODIFY COLUMN `a.b` Tuple(x Nullable(UInt64));
KILL MUTATION WHERE table = 't_absent_bool_dotted' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 150;
SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 24. a SERIALIZATION-DEFINED subcolumn whose parent the part does not carry refuses too';
-- Case 22 for the subcolumns case 21 lets through: `vec.8` is a `QBit` bit plane, defined by the
-- parent's custom serialization - which columns.txt DOES round-trip here, because QBit sets it from
-- its own type (unlike Quantized, whose serialization comes from the codec that columns.txt drops).
-- So the reason to refuse is not unrepresentability but parent ABSENCE: this part holds no `vec` at
-- all, and its granule was written for `QBit(Float32, 4)`. Waving the subcolumn through on the
-- strength of its parent's serialization would skip the type check and prune with a stale granule.
-- The backticked spelling is what makes the index require the SUBCOLUMN `vec.8`: it is one
-- identifier, so it resolves against the subcolumn-aware column list, while an unbackticked `vec.8`
-- is the dot operator and requires the physical parent `vec` instead.
DROP TABLE IF EXISTS t_absent_qbit_sub;
CREATE TABLE t_absent_qbit_sub (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_qbit_sub SELECT number, toString(number) FROM numbers(64);
ALTER TABLE t_absent_qbit_sub ADD COLUMN vec QBit(Float32, 4)
DEFAULT arrayMap(x -> toFloat32(k + x), range(4))::QBit(Float32, 4) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_qbit_sub ADD INDEX idx `vec.8` TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_qbit_sub MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_qbit_sub' AND active AND column = 'vec';
SYSTEM STOP MERGES t_absent_qbit_sub;
ALTER TABLE t_absent_qbit_sub MODIFY COLUMN vec QBit(Float64, 4);
KILL MUTATION WHERE table = 't_absent_qbit_sub' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_qbit_sub WHERE `vec.8` = CAST(unhex('00'), 'FixedString(1)');
SELECT count() FROM t_absent_qbit_sub WHERE `vec.8` = CAST(unhex('00'), 'FixedString(1)') SETTINGS use_skip_indexes = 0;

SELECT '-- 25. over-fire control: a backticked QBit subcolumn index prunes when the part carries the parent';
-- The other side of case 24: same backticked `vec.8` index, but the parent is present and no type is
-- stale, so the index must still prune. Case 24 alone cannot tell a correct refusal apart from this
-- spelling never pruning at all - only the pair does. `04403` covers the UNBACKTICKED `vec.8`, which
-- is the dot operator and so requires the physical parent instead of this subcolumn.
DROP TABLE IF EXISTS t_keep_qbit_sub;
CREATE TABLE t_keep_qbit_sub (k UInt64, vec QBit(Float32, 4),
    INDEX idx `vec.8` TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_qbit_sub SELECT number, arrayMap(x -> toFloat32(number + x), range(4))::QBit(Float32, 4) FROM numbers(64);
SYSTEM STOP MERGES t_keep_qbit_sub;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_keep_qbit_sub' AND active AND column = 'vec';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_qbit_sub WHERE `vec.8` = CAST(unhex('02'), 'FixedString(1)')) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_qbit_sub WHERE `vec.8` = CAST(unhex('02'), 'FixedString(1)');
SELECT count() FROM t_keep_qbit_sub WHERE `vec.8` = CAST(unhex('02'), 'FixedString(1)') SETTINGS use_skip_indexes = 0;

DROP TABLE t_stale_nullable;
DROP TABLE t_stale_plain;
DROP TABLE t_stale_json;
DROP TABLE t_stale_expr;
DROP TABLE t_stale_enum;
DROP TABLE t_stale_minmax;
DROP TABLE t_keep_date;
DROP TABLE t_keep_enum;
DROP TABLE t_keep_plain;
DROP TABLE t_name_reuse;
DROP TABLE t_absent_col;
DROP TABLE t_pre_add_index;
DROP TABLE t_materialized_index;
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
DROP TABLE t_absent_sub;
DROP TABLE t_absent_bool_prefix;
DROP TABLE t_absent_bool_dotted;
DROP TABLE t_absent_qbit_sub;
DROP TABLE t_keep_qbit_sub;
