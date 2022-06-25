DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt32, b UInt32) ENGINE=Memory;
INSERT INTO t VALUES (123, 42);

SET query_cache_active_usage = true;
SET query_cache_passive_usage = true;
SET min_query_runs_before_caching = 3;

-- query results are no longer in cache after drop
SELECT b from t;
SELECT b from t;
SELECT b from t;

EXPLAIN SELECT b from t;
SYSTEM DROP QUERY CACHE;
EXPLAIN SELECT b from t;

-- timers for removal are removed after drop
SET query_cache_entry_put_timeout_ms = 1000; -- 1 sec
SELECT a from t;
SELECT a from t;
SELECT a from t;
SELECT sleep(0.4);
SYSTEM DROP QUERY CACHE;
SELECT a from t;
SELECT a from t;
SELECT a from t;
SELECT sleep(0.7);
EXPLAIN SELECT a from t;
