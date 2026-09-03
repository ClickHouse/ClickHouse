-- Tags: no-replicated-database, no-shared-merge-tree

-- A granularity close to the maximum of `UInt64` must still return the rows that were written, for the
-- data marks of a part with non-adaptive granularity, for the granules of a secondary index, when a
-- top-K read budgets granules across parts, and when the mark ranges of a reverse read are split.

DROP TABLE IF EXISTS t_granularity_near_max;

-- Non-adaptive granularity stores wide parts only, so both wide-part thresholds must be 0.
CREATE TABLE t_granularity_near_max (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity_bytes = 0, index_granularity = 18446744073709551615, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_granularity_near_max SELECT number FROM numbers(10);

-- Pinned local: a replica-side read still holding the table makes the `ATTACH` below fail.
SELECT 'written', count(), sum(x) FROM t_granularity_near_max
SETTINGS optimize_trivial_count_query = 0, enable_parallel_replicas = 0;
SELECT 'read', x FROM t_granularity_near_max ORDER BY x SETTINGS enable_parallel_replicas = 0;

-- Reloading the table must see the same rows the insert wrote.
DETACH TABLE t_granularity_near_max;
ATTACH TABLE t_granularity_near_max;

SELECT 'reloaded', count(), sum(x) FROM t_granularity_near_max SETTINGS optimize_trivial_count_query = 0;

DROP TABLE t_granularity_near_max;

DROP TABLE IF EXISTS t_skip_granularity_near_max;

-- An index `GRANULARITY` counts marks, so the part needs more than one mark for the rounding to matter.
CREATE TABLE t_skip_granularity_near_max (x UInt64, y UInt64, INDEX i y TYPE minmax GRANULARITY 18446744073709551615)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity = 64;

INSERT INTO t_skip_granularity_near_max SELECT number, number FROM numbers(4096);

SELECT 'skip index, no filter', count(), sum(y) FROM t_skip_granularity_near_max;
SELECT 'skip index, filtered', count(), sum(y) FROM t_skip_granularity_near_max WHERE y = 500 SETTINGS force_data_skipping_indices = 'i';

-- A top-K read consults the index while it reads rows, through a different code path than the filter
-- above. The settings keep that path armed while the suite randomizes them.
SELECT 'skip index, top-K read', y FROM t_skip_granularity_near_max ORDER BY y LIMIT 1
SETTINGS max_block_size = 64, max_threads = 1, use_skip_indexes_on_data_read = 1,
         use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
         enable_parallel_replicas = 0;

-- That granule starts at the part's smallest value, so a top-K threshold can never exclude it, and no
-- row or mark count separates a read that consulted the granules from one that did not. This arm only
-- asserts the granule filter is selected for `MergeTreeDataSelectExecutor::getMinMaxIndexGranules`.
SELECT 'skip index, top-K granule filter', countIf(explain LIKE '%Filter TopK Granules%')
FROM (EXPLAIN indexes = 1 SELECT y FROM t_skip_granularity_near_max ORDER BY y LIMIT 1
      SETTINGS max_block_size = 64, max_threads = 1, use_skip_indexes_on_data_read = 1,
               use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1,
               enable_parallel_replicas = 0);

-- One index granule spans the whole part here, and it holds the matching value, so no granule may be
-- dropped. Only the prefix is matched: the number of marks depends on randomized settings. Part
-- pruning by column statistics is off, so the index is the only thing that can drop a granule.
-- Every arm in this file that reads plan text pins parallel replicas off: with
-- `parallel_replicas_local_plan = 0` the initiator plans no local read step, so there is no index
-- description to count.
SELECT 'skip index keeps every granule', countIf(explain LIKE '%Granules: 0/%')
FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_skip_granularity_near_max WHERE y = 500
      SETTINGS use_statistics_for_part_pruning = 0, enable_parallel_replicas = 0);

-- No granule can hold this value, so the index has to drop them all. This reads 0 when the index is
-- never consulted, which is the case the line above cannot tell apart on its own.
SELECT 'skip index drops every granule', countIf(explain LIKE '%Granules: 0/%')
FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_skip_granularity_near_max WHERE y = 999999
      SETTINGS use_statistics_for_part_pruning = 0, enable_parallel_replicas = 0);

SELECT 'skip index, no match', count(), sum(y) FROM t_skip_granularity_near_max WHERE y = 999999
SETTINGS force_data_skipping_indices = 'i', use_statistics_for_part_pruning = 0;

DROP TABLE t_skip_granularity_near_max;

DROP TABLE IF EXISTS t_topk_across_parts;

-- A top-K read keeps at least `LIMIT` times the index `GRANULARITY` granules across all parts. Two
-- parts whose granule minimum differs are needed: the smaller minimum takes that whole budget, so a
-- budget smaller than the first part's granule count drops the second part entirely.
CREATE TABLE t_topk_across_parts (x UInt64, y UInt64, INDEX i y TYPE minmax GRANULARITY 18446744073709551615)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES t_topk_across_parts;

INSERT INTO t_topk_across_parts SELECT number * 2, number * 2 FROM numbers(2);
INSERT INTO t_topk_across_parts SELECT number * 2 + 1, number * 2 + 1 FROM numbers(2);

SELECT 'top-K across parts', y FROM t_topk_across_parts ORDER BY y LIMIT 18446744073709551615
SETTINGS max_threads = 1, use_skip_indexes_on_data_read = 1, use_skip_indexes_for_top_k = 1,
         use_top_k_dynamic_filtering = 1, enable_parallel_replicas = 0,
         query_plan_max_limit_for_top_k_optimization = 0;

-- Without the granule filter the arm above returns every row, so it cannot fail on its own. This
-- counts the filter on the same query.
SELECT 'top-K across parts, granule filter', countIf(explain LIKE '%Filter TopK Granules%')
FROM (EXPLAIN indexes = 1 SELECT y FROM t_topk_across_parts ORDER BY y LIMIT 18446744073709551615
      SETTINGS max_threads = 1, use_skip_indexes_on_data_read = 1, use_skip_indexes_for_top_k = 1,
               use_top_k_dynamic_filtering = 1, enable_parallel_replicas = 0,
               query_plan_max_limit_for_top_k_optimization = 0);

DROP TABLE t_topk_across_parts;

DROP TABLE IF EXISTS t_skip_granularity_one;

CREATE TABLE t_skip_granularity_one (x UInt64, y UInt64, INDEX i y TYPE minmax GRANULARITY 1)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity = 64;

INSERT INTO t_skip_granularity_one SELECT number, number FROM numbers(4096);

SELECT 'skip index granularity 1, filtered', count(), sum(y) FROM t_skip_granularity_one WHERE y = 500 SETTINGS force_data_skipping_indices = 'i';

-- Here one index granule covers one mark, so exactly the mark holding the matching value survives.
-- This is the control that shows the count above can observe a granule being dropped.
SELECT 'skip index drops all but one granule', countIf(explain LIKE '%Granules: 1/%')
FROM (EXPLAIN indexes = 1 SELECT sum(y) FROM t_skip_granularity_one WHERE y = 500
      SETTINGS enable_parallel_replicas = 0);

DROP TABLE t_skip_granularity_one;

DROP TABLE IF EXISTS t_reverse_granularity_near_max;

-- Adaptive granularity keeps more than one mark in the part, which is what a reverse read splits.
CREATE TABLE t_reverse_granularity_near_max (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity = 18446744073709551615, index_granularity_bytes = 1024;

INSERT INTO t_reverse_granularity_near_max SELECT number FROM numbers(4096);

-- Splitting the mark ranges of a reverse read must terminate. The memory limit bounds a
-- non-terminating split, so it fails instead of running until the server is out of memory.
SELECT 'reverse', x FROM t_reverse_granularity_near_max ORDER BY x DESC LIMIT 3
SETTINGS optimize_read_in_order = 1, max_threads = 1, max_memory_usage = 200000000,
         enable_parallel_replicas = 0;

-- An ordinary sort returns those three rows as well, so this asserts that the plan which produced
-- them is the one that splits mark ranges backwards.
SELECT 'reverse reads in reverse order', countIf(explain LIKE '%Read type: InReverseOrder%')
FROM (EXPLAIN actions = 1 SELECT x FROM t_reverse_granularity_near_max ORDER BY x DESC LIMIT 3
      SETTINGS optimize_read_in_order = 1, max_threads = 1, enable_parallel_replicas = 0);

DROP TABLE t_reverse_granularity_near_max;
