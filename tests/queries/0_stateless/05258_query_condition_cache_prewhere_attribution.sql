-- The query condition cache must record the static WHERE conditions of a join whose PREWHERE also holds
-- a join runtime filter, and it must never record granules that only the runtime filter emptied.

SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build;
DROP TABLE IF EXISTS widened;
DROP TABLE IF EXISTS probe_small_key;
DROP TABLE IF EXISTS build_small_key;

-- The join key `k` is not in the sorting key, so only the query condition cache can skip granules of
-- `probe`. Each arm below uses its own IN list, so each arm has its own cache entries.
CREATE TABLE probe (k UInt64, pad String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0;
INSERT INTO probe SELECT number, repeat('x', 16) FROM numbers(20000);

-- A build side that is not a MergeTree table neither reads nor writes the query condition cache.
CREATE TABLE build (k UInt64, pad String) ENGINE = Memory;
INSERT INTO build SELECT k, repeat('x', 16) FROM (SELECT arrayJoin([7, 100, 5000, 12345, 101, 5001, 12346, 107, 5007, 12352]) AS k);

SELECT 'the runtime filter shares PREWHERE with the static condition';
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0
    SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12345))
WHERE explain ILIKE '%Prewhere filter column: and(in(%k, %), \_\_applyFilter(%k))%';

SELECT 'join with a runtime filter';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12345)
SETTINGS log_comment = 'qcc_rf_1';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12345)
SETTINGS log_comment = 'qcc_rf_2';

SELECT 'the same with index analysis of the runtime filter disabled';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12346)
SETTINGS enable_join_runtime_filters_index_analysis = 0, log_comment = 'qcc_rf_no_index_analysis_1';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12346)
SETTINGS enable_join_runtime_filters_index_analysis = 0, log_comment = 'qcc_rf_no_index_analysis_2';

SELECT 'control: the join without a runtime filter or PREWHERE';
-- Without PREWHERE only blocks that WHERE empties are recorded, hence one granule per block.
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (101, 5001, 12346)
SETTINGS enable_join_runtime_filters = 0, optimize_move_to_prewhere = 0, max_block_size = 128, log_comment = 'qcc_no_rf_1';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (101, 5001, 12346)
SETTINGS enable_join_runtime_filters = 0, optimize_move_to_prewhere = 0, max_block_size = 128, log_comment = 'qcc_no_rf_2';

SELECT 'control: the cache disabled';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12345)
SETTINGS use_query_condition_cache = 0, log_comment = 'qcc_no_cache';

SELECT 'control: no join';
SELECT count(), sum(length(pad)) FROM probe WHERE k IN (103, 5003, 12348) SETTINGS log_comment = 'qcc_no_join_1';
SELECT count(), sum(length(pad)) FROM probe WHERE k IN (103, 5003, 12348) SETTINGS log_comment = 'qcc_no_join_2';

SELECT 'granules emptied by the runtime filter alone are not recorded';
-- Only key 100 of the IN list is on the build side, so the runtime filter empties the granules of 5004 and
-- 12349, which the IN list alone does not. Small blocks make their granules empty blocks. The always-true
-- `p.k != b.k + 1` is pushed into PREWHERE after the runtime filter, which is kept enabled although the
-- first row it checks passes. The counts below consult the cache entry the join wrote.
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k AND p.k != b.k + 1
WHERE p.k IN (100, 5004, 12349)
SETTINGS max_block_size = 128, join_runtime_filter_pass_ratio_threshold_for_disabling = 1;
SELECT count() FROM probe WHERE k IN (100, 5004, 12349) SETTINGS log_comment = 'qcc_rf_emptied_check';
SELECT count() FROM probe WHERE k IN (100, 5004, 12349) SETTINGS use_query_condition_cache = 0;

SELECT 'a condition widened after index analysis is not attributed';
-- Push-down adds the always-false `ON` conjunct to the PREWHERE of both sides after the cache key was
-- fixed, so the runtime filter built from the empty build side empties the probe granule that `v > 5`
-- matches 194 rows of.
CREATE TABLE widened (v Int64, pad String) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 8192;
INSERT INTO widened SELECT number, repeat('x', 16) FROM numbers(200);
SELECT count(), sum(length(widened.pad)), sum(length(a.pad)) FROM widened LOCAL RIGHT JOIN widened AS a
    ON and(equals(v, a.v), not(equals(v, a.v))) WHERE v > 5
SETTINGS query_plan_convert_outer_join_to_inner_join = 1, enable_join_runtime_filters_index_analysis = 0;
SELECT count() FROM widened WHERE v > 5 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM widened WHERE v > 5 SETTINGS use_query_condition_cache = 0;

SELECT 'explicit PREWHERE extended by push-down';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k PREWHERE p.pad != '' WHERE p.k IN (100, 5006, 12345)
SETTINGS optimize_prewhere_after_pushdown = 1;
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k PREWHERE p.pad != '' WHERE p.k IN (100, 5006, 12345)
SETTINGS optimize_prewhere_after_pushdown = 1;
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k PREWHERE p.pad != '' WHERE p.k IN (100, 5006, 12345)
SETTINGS optimize_prewhere_after_pushdown = 1, use_query_condition_cache = 0;

SELECT 'runtime filters of a multi-key join and of an ANTI join';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k AND p.pad = b.pad WHERE p.k IN (100, 5000, 12352, 13);
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k AND p.pad = b.pad WHERE p.k IN (100, 5000, 12352, 13);
SELECT count(), sum(length(p.pad)) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12352, 13);
SELECT count(), sum(length(p.pad)) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12352, 13);
SELECT count(), sum(length(p.pad)) FROM probe AS p LEFT ANTI JOIN build AS b ON p.k = b.k WHERE p.k IN (100, 5000, 12352, 13)
SETTINGS use_query_condition_cache = 0;

SELECT 'a PREWHERE read in a single step records nothing';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (107, 5007, 12352)
SETTINGS enable_multiple_prewhere_read_steps = 0, log_comment = 'qcc_single_step_1';
SELECT count(), sum(length(p.pad)) FROM probe AS p INNER JOIN build AS b ON p.k = b.k WHERE p.k IN (107, 5007, 12352)
SETTINGS enable_multiple_prewhere_read_steps = 0, log_comment = 'qcc_single_step_2';

SELECT 'a runtime filter ordered before the static condition';
-- The small join key comes first in PREWHERE, so there is nothing to attribute.
CREATE TABLE probe_small_key (j UInt8, s String, pad String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, add_minmax_index_for_numeric_columns = 0;
INSERT INTO probe_small_key SELECT number % 200, toString(number), repeat('x', 16) FROM numbers(20000);
CREATE TABLE build_small_key (j UInt8) ENGINE = Memory;
INSERT INTO build_small_key VALUES (1), (2), (3);
SELECT count(), sum(length(p.pad)) FROM probe_small_key AS p INNER JOIN build_small_key AS b ON p.j = b.j WHERE p.s IN ('1', '2', '202', '5003');
SELECT count(), sum(length(p.pad)) FROM probe_small_key AS p INNER JOIN build_small_key AS b ON p.j = b.j WHERE p.s IN ('1', '2', '202', '5003');
SELECT count(), sum(length(p.pad)) FROM probe_small_key AS p INNER JOIN build_small_key AS b ON p.j = b.j WHERE p.s IN ('1', '2', '202', '5003')
SETTINGS use_query_condition_cache = 0;

SELECT 'query condition cache hits and pruned reads';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0 AS hit, read_rows < 20000 AS pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE 'qcc\_%'
ORDER BY log_comment;

DROP TABLE probe;
DROP TABLE build;
DROP TABLE widened;
DROP TABLE probe_small_key;
DROP TABLE build_small_key;
