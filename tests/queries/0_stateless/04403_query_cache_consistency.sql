-- Tests for query_cache_use_only_when_data_was_not_changed (issue #108713).
-- When enabled, a cached query result is reused only while the referenced tables are unchanged.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2);

SYSTEM DROP QUERY CACHE;

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

SYSTEM DROP QUERY CACHE;
DROP TABLE t;
