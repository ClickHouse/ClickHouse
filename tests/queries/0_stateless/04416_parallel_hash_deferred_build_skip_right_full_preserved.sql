-- Tags: no-random-settings, no-random-merge-tree-settings

-- Correctness guard for the deferred parallel_hash "skip discarded right blocks" change: RIGHT and
-- FULL joins must keep their non-joined right rows. The skip mirrors the streaming build's pop, which
-- only fires for `!isRightOrFull(kind)` - a RIGHT/FULL build stores a nullmap for the not-joined rows
-- (so `nullmap_stored_for_block` is true and the block is never popped). `buildPopsZeroInsertBlocks`
-- excludes RIGHT/FULL, so the deferred build must buffer every block, and a right row that is NULL-key
-- or fails the ON mask must still appear in the output. parallel_hash must equal hash row for row.

SET collect_hash_table_stats_during_joins = 0; -- no size hint => the deferred build path
SET parallel_hash_join_threshold = 0;          -- force ConcurrentHashJoin regardless of build size
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET query_plan_join_swap_table = 'false';      -- keep the big nullable table as the build (right) side
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_rf_build;
DROP TABLE IF EXISTS t_rf_probe;
CREATE TABLE t_rf_build (k Nullable(UInt64), keep UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

-- Right side: k % 7 = 0 -> NULL key; keep = 1 only for k < 2000. So most right rows are non-joined
-- (NULL key or ON-filtered) - exactly the rows a buggy skip would drop. They must all survive.
INSERT INTO t_rf_build SELECT if(number % 7 = 0, NULL, number), (number < 2000) AS keep, number * 3 FROM numbers(300000);
INSERT INTO t_rf_probe SELECT number, number * 7 FROM numbers(300000);

SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l RIGHT JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash', max_bytes_in_join = 0;

SELECT 'right', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l RIGHT JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash', max_bytes_in_join = 0;

SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l FULL JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'hash', max_bytes_in_join = 0;

SELECT 'full', count(), sum(cityHash64(l.k, l.v, r.k, r.keep, r.v))
FROM t_rf_probe l FULL JOIN t_rf_build r ON l.k = r.k AND r.keep = 1
SETTINGS join_algorithm = 'parallel_hash', max_bytes_in_join = 0;

DROP TABLE t_rf_build;
DROP TABLE t_rf_probe;
