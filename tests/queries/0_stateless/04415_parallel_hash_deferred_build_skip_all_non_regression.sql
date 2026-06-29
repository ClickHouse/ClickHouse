-- Tags: no-random-settings, no-random-merge-tree-settings

-- Non-regression for the deferred parallel_hash "skip discarded right blocks" change: it must NOT
-- touch ALL strictness. The skip fires only when the slot's HashJoin would pop a zero-insert block -
-- a one-row-per-key MapsOne (ANY / SEMI / ANTI) INNER/LEFT build. ALL strictness builds MapsAll
-- (RowRefList), seeds `is_inserted = true` and never pops, so `HashJoin::buildPopsZeroInsertBlocks`
-- is false and nothing is skipped. Over the same right-side ON filter as 04414, ALL LEFT under
-- parallel_hash must be byte-for-byte identical to hash (both for an enforced cap and unbounded), and
-- a duplicate-heavy ALL INNER must keep every matching row.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0;          -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_skip_build;
DROP TABLE IF EXISTS t_skip_probe;
CREATE TABLE t_skip_build (k UInt64, keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_skip_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_skip_build SELECT number, (number < 2000) AS keep, number * 3 FROM numbers(1000000);
INSERT INTO t_skip_probe SELECT number, number * 7 FROM numbers(1000000);

-- ALL LEFT with the same ON filter, enforced cap (above the empty-map baseline): hash and
-- parallel_hash must agree exactly. The skip must not have dropped (or kept) anything differently.
SELECT 'all_left', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash', join_overflow_mode = 'throw', max_bytes_in_join = 16000000;

SELECT 'all_left', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash', join_overflow_mode = 'throw', max_bytes_in_join = 16000000;

-- Unbounded: same agreement without the cap.
SELECT 'all_left_nolimit', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash', max_bytes_in_join = 0;

SELECT 'all_left_nolimit', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_skip_probe l ALL LEFT JOIN t_skip_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash', max_bytes_in_join = 0;

-- Duplicate-heavy ALL INNER: 10 distinct build keys, 50000 rows each, must emit every matching row
-- (the RowRefList path). A wrongful skip would drop matches and change the count - both algorithms
-- must agree.
DROP TABLE IF EXISTS t_dup_build;
CREATE TABLE t_dup_build (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dup_build SELECT number % 10, number FROM numbers(500000);

SELECT 'all_inner_dup', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_build r ON l.k = r.k
SETTINGS join_algorithm = 'hash', max_bytes_in_join = 0;

SELECT 'all_inner_dup', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number AS k FROM numbers(10)) l ALL INNER JOIN t_dup_build r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_hash', max_bytes_in_join = 0;

DROP TABLE t_skip_build;
DROP TABLE t_skip_probe;
DROP TABLE t_dup_build;
