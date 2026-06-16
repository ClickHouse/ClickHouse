-- The deferred exact-size parallel_hash build must engage when max_rows_in_join / max_bytes_in_join
-- are set (the CI default profile sets both to 10G) and must enforce them with the same contract as
-- the streaming build: the limit is checked against the distinct-key map cells (not the buffered
-- source rows) and the real byte count (which includes the BuildRefList arena nodes), enforced after
-- the replay against the real maps. `throw` aborts the query; `break` cannot be honored after a full
-- replay, so the deferral is disabled for it and the streaming build stops filling the right side.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET parallel_hash_join_threshold = 0;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS t_build_lim;
DROP TABLE IF EXISTS t_probe_lim;

CREATE TABLE t_build_lim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE t_probe_lim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();

INSERT INTO t_build_lim SELECT number, number * 7 FROM numbers(200000);
INSERT INTO t_probe_lim SELECT 100000 + intDiv(number, 2), number * 13 FROM numbers(200000);

-- Benign limits (the shape of the CI default profile): results must match the hash algorithm.
SELECT 'inner_with_limits', count(), sum(cityHash64(l.k, l.v, r.v))
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS join_algorithm = 'hash', max_rows_in_join = 1000000000, max_bytes_in_join = 1000000000;

SELECT 'inner_with_limits', count(), sum(cityHash64(l.k, l.v, r.v))
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', max_rows_in_join = 1000000000, max_bytes_in_join = 1000000000,
         log_comment = '04335_parallel_hash_deferred_build_size_limits';

-- Duplicate-heavy build: `max_rows_in_join` is checked against distinct-key cells, not source rows.
-- 10 distinct keys with 5000 rows each give 50000 source rows but only 10 hash-map cells, so a
-- `max_rows_in_join` of 1000 (far above the 10 cells, far below the 50000 source rows) must NOT
-- reject the join. Both algorithms must succeed and agree; before the fix the deferred build threw
-- here because the buffering-time check saw the buffered source-row count.
DROP TABLE IF EXISTS t_dup_lim;
CREATE TABLE t_dup_lim (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO t_dup_lim SELECT number % 10, number FROM numbers(50000);

SELECT 'dup_rows_below_cells_limit', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_lim r ON l.k = r.k
SETTINGS join_algorithm = 'hash', max_rows_in_join = 1000, join_overflow_mode = 'throw';

SELECT 'dup_rows_below_cells_limit', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_lim r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', max_rows_in_join = 1000, join_overflow_mode = 'throw';

-- Limits the build side cannot satisfy: 200000 distinct keys far exceed these limits, so the deferred
-- build must fail the query in `throw` mode. Enforced after the replay against the real map cells and
-- the real byte count (which includes the BuildRefList arena nodes).
SELECT count()
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', join_overflow_mode = 'throw', max_rows_in_join = 1000; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT count()
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', join_overflow_mode = 'throw', max_bytes_in_join = 1024; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- `break` cannot be reproduced after a full deferred replay, so the deferral is disabled for it and
-- the streaming build stops filling the right side without an error. The number of joined rows depends
-- on how many blocks were in flight when the limit tripped, so only the success is checked.
SELECT 'break_succeeds', count() >= 0
FROM t_probe_lim l INNER JOIN t_build_lim r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', join_overflow_mode = 'break', max_rows_in_join = 1000;

-- Positive control: the benign-limit parallel_hash query must have used the deferred exact-size
-- reserve (`HashJoinDeferredPreallocatedElementsInHashTables` is incremented only by that reserve;
-- the statistics-driven reserve is off because statistics collection is disabled).
SYSTEM FLUSH LOGS query_log;
SELECT
    'deferred build engaged with limits',
    count() > 0,
    countIf(ProfileEvents['HashJoinDeferredPreallocatedElementsInHashTables'] > 0) = count()
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = '04335_parallel_hash_deferred_build_size_limits';

DROP TABLE t_build_lim;
DROP TABLE t_probe_lim;
DROP TABLE t_dup_lim;
