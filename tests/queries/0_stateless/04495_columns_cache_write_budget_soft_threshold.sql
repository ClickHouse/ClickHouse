-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- columns_cache_max_bytes_to_write_to_cache is a soft per-query threshold, not a
-- hard cap: the entry that crosses it (including a single large first entry while
-- the counter is still zero) is written in full before the counter is charged.
-- With the budget set to 1 byte, the first cache entry is therefore still written,
-- so the cache ends up populated rather than empty.

SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_soft_budget;

CREATE TABLE t_cc_soft_budget (k UInt64, payload String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO t_cc_soft_budget SELECT number, randomPrintableASCII(60) FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- A budget of 1 byte is below any single cache entry. The soft threshold still
-- writes the first entry in full (documented one-entry overshoot), so the cache
-- is populated rather than empty.
SELECT sum(k) > 0 FROM t_cc_soft_budget
SETTINGS use_columns_cache = 1, columns_cache_max_bytes_to_write_to_cache = 1;

SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase();

DROP TABLE t_cc_soft_budget;
