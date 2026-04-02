-- Tags: no-random-settings, no-random-merge-tree-settings

-- Test that lazy FINAL optimization applies index analysis at runtime
-- to narrow down the read ranges for the optimized branch.
-- The filter is on a non-key column, so normal PK pruning doesn't help FINAL.
-- But the optimization builds a set of matching sorting keys, allowing
-- the LazyReadReplacingFinalSource to read fewer parts.

DROP TABLE IF EXISTS t_lazy_final_index;

CREATE TABLE t_lazy_final_index
(
    key UInt64,
    version UInt64,
    status String,
    payload String
)
ENGINE = ReplacingMergeTree(version)
ORDER BY key
SETTINGS index_granularity = 64;

-- Insert 5 non-overlapping parts. Only part 1 (keys 0..999) has status='target'.
INSERT INTO t_lazy_final_index SELECT number, 1, if(number < 100, 'target', 'other'), repeat('x', 100) FROM numbers(1000);
INSERT INTO t_lazy_final_index SELECT number + 1000, 1, 'other', repeat('x', 100) FROM numbers(1000);
INSERT INTO t_lazy_final_index SELECT number + 2000, 1, 'other', repeat('x', 100) FROM numbers(1000);
INSERT INTO t_lazy_final_index SELECT number + 3000, 1, 'other', repeat('x', 100) FROM numbers(1000);
INSERT INTO t_lazy_final_index SELECT number + 4000, 1, 'other', repeat('x', 100) FROM numbers(1000);

SYSTEM STOP MERGES t_lazy_final_index;

-- Correctness: results must match.
SELECT '-- correctness';
SELECT count(), sum(length(payload)) FROM t_lazy_final_index FINAL WHERE status = 'target'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(length(payload)) FROM t_lazy_final_index FINAL WHERE status = 'target'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000;

SYSTEM FLUSH LOGS;

-- Without optimization: FINAL reads all 5 parts (filter on status can't prune by PK).
SELECT '-- selected parts without optimization';
SELECT ProfileEvents['SelectedParts']
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%FROM t_lazy_final_index FINAL WHERE status%'
    AND query LIKE '%query_plan_optimize_lazy_final = 0%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- With optimization: the set-building read reads all 5 parts, but
-- LazyReadReplacingFinalSource should use index analysis to read only 1 part
-- (the one containing keys 0..99). Total SelectedParts should be < 15.
SELECT '-- selected parts with optimization';
SELECT ProfileEvents['SelectedParts']
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%FROM t_lazy_final_index FINAL WHERE status%'
    AND query LIKE '%query_plan_optimize_lazy_final = 1%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The optimized path reads fewer parts for the FINAL branch (even though total
-- SelectedParts includes the set-building read).
-- Without optimization: 5 parts.
-- With optimization: 5 (set read) + 1 (index-pruned FINAL) + 5 (lazy read) = 11.
-- This confirms index analysis narrowed the FINAL read from 5 to 1 part.

DROP TABLE t_lazy_final_index;
