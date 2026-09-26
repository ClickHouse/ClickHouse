-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Lazy materialization reads the columns that the `ORDER BY ... LIMIT` does not need in a second
-- pass, for the surviving rows only, and that pass builds its own read pool in
-- `LazyReadFromMergeTreeSource` rather than going through `ReadFromMergeTree`. The columns cache
-- has to be wired into that pool too: the ordinary cache tests never reach it, because a query
-- whose plan is `LazilyReadFromMergeTree` reads the sort and output columns in the first pass and
-- the payload only in the second one.

SET enable_analyzer = 1;
SET max_threads = 1;
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000;
SET log_queries = 1;

DROP TABLE IF EXISTS t_cc_lazy;

CREATE TABLE t_cc_lazy (a UInt64, b String, payload String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024, index_granularity_bytes = 0;

SYSTEM STOP MERGES t_cc_lazy;
INSERT INTO t_cc_lazy SELECT number, toString(number % 1000), repeat('p', 100) || toString(number) FROM numbers(100000);

-- The second pass exists only if the plan really is the lazy one; otherwise everything below
-- would be an ordinary read and would prove nothing about `LazyReadFromMergeTreeSource`.
SELECT 'lazy plan', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN PLAN SELECT a, payload FROM t_cc_lazy ORDER BY b, a LIMIT 5);

SYSTEM DROP COLUMNS CACHE;

-- Cold: the second pass writes what it reads. Warm: it serves the same rows from the cache.
SELECT 'cold', a, b, substring(payload, 1, 4) FROM t_cc_lazy ORDER BY b, a LIMIT 5
SETTINGS use_columns_cache = 1, log_comment = '05230_lazy_cold';
SELECT 'warm', a, b, substring(payload, 1, 4) FROM t_cc_lazy ORDER BY b, a LIMIT 5
SETTINGS use_columns_cache = 1, log_comment = '05230_lazy_warm';

-- The same answer without the cache at all.
SELECT 'no cache', a, b, substring(payload, 1, 4) FROM t_cc_lazy ORDER BY b, a LIMIT 5
SETTINGS use_columns_cache = 0;

-- With reads from the cache disabled the warm read misses again, so the hit below is attributable
-- to the read path of the second pass and not to some other query of this test.
SELECT 'reads disabled', a, b, substring(payload, 1, 4) FROM t_cc_lazy ORDER BY b, a LIMIT 5
SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 0, log_comment = '05230_lazy_reads_disabled';

-- The payload column, which only the second pass reads, is in the cache.
SELECT 'cached columns', arraySort(groupUniqArray(column)) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cc_lazy';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits, ProfileEvents['ColumnsCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05230_lazy_%'
ORDER BY log_comment;

DROP TABLE t_cc_lazy;
