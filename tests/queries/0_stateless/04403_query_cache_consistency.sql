-- Tests for query_cache_use_only_when_data_was_not_changed (issue #108713).
-- When enabled, a cached query result is reused only while the referenced tables are unchanged.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2);

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache (which would require a no-parallel tag).

-- With the consistency setting, the result is always fresh: the cache is reused on the second run,
-- but a new INSERT invalidates it and the third run recomputes the up-to-date result.
SELECT count() FROM t SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() FROM t SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO t VALUES (3);
SELECT count() FROM t SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- Without the consistency setting (the default), a stale result is served after the data changes.
SELECT count() FROM t SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;
INSERT INTO t VALUES (4);
SELECT count() FROM t SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;

DROP TABLE t;

-- A metadata-only ALTER MODIFY ORDER BY changes FINAL results without rewriting parts: the sorting key
-- is the deduplication key of the ReplacingMergeTree family. The consistency setting must still serve a
-- fresh result. Two separate inserts (two parts) so FINAL deduplicates across them.
DROP TABLE IF EXISTS t_final;
CREATE TABLE t_final (k UInt64, v UInt64) ENGINE = ReplacingMergeTree PRIMARY KEY k ORDER BY (k, v);
INSERT INTO t_final VALUES (1, 10);
INSERT INTO t_final VALUES (1, 20);
-- With ORDER BY (k, v) the rows are distinct: count is 2 (cached on the second run).
SELECT count() FROM t_final FINAL SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() FROM t_final FINAL SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
ALTER TABLE t_final MODIFY ORDER BY k;
-- Now FINAL deduplicates on k, so the fresh result is 1, not the stale cached 2.
SELECT count() FROM t_final FINAL SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

DROP TABLE t_final;
