-- Tags: no-fasttest, no-random-merge-tree-settings, no-parallel-replicas
-- no-fasttest: the JSON case needs the JSON type.
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block), and use_skip_indexes_on_data_read is not supported with parallel replicas.
-- Cases 11-29 of this series live in 04869_skip_index_stale_type_absent_column,
-- 04870_skip_index_stale_type_metadata_only_alter and 04871_skip_index_stale_type_subcolumns: one
-- test exceeded the flaky-check runtime limit under sanitizers, so it is split, keeping the original
-- case numbering.

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
-- A lazy type-hint ALTER over a subcolumn a skip index stores is refused, so no mutation is
-- created and the index cannot go stale by this route.
ALTER TABLE t_stale_json MODIFY COLUMN j JSON(a UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM system.mutations WHERE table = 't_stale_json' AND database = currentDatabase();
-- With the hint applied as a full mutation, killing it leaves the index holding String while the
-- metadata says UInt64, which is the state the read path must refuse to deserialize.
DROP TABLE IF EXISTS t_stale_json_killed;
CREATE TABLE t_stale_json_killed (k UInt64, j JSON, INDEX idx j.a TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_stale_json_killed SELECT number, toJSONString(map('a', toString(number * 3))) FROM numbers(64);
SYSTEM STOP MERGES t_stale_json_killed;
ALTER TABLE t_stale_json_killed MODIFY COLUMN j JSON(a UInt64) SETTINGS allow_experimental_json_lazy_type_hints = 0;
KILL MUTATION WHERE table = 't_stale_json_killed' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_stale_json_killed WHERE j.a = 150;

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

DROP TABLE t_stale_nullable;
DROP TABLE t_stale_plain;
DROP TABLE t_stale_json;
DROP TABLE t_stale_json_killed;
DROP TABLE t_stale_expr;
DROP TABLE t_stale_enum;
DROP TABLE t_stale_minmax;
DROP TABLE t_keep_date;
DROP TABLE t_keep_enum;
DROP TABLE t_keep_plain;
DROP TABLE t_name_reuse;
