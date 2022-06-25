DROP TABLE IF EXISTS t;

CREATE TABLE t (n UInt32) ENGINE=Memory;
INSERT INTO t VALUES (1);

SET query_cache_entry_put_timeout_ms = 1000050000;
SET min_query_runs_before_caching = 0;

-- no caching at all
SET query_cache_active_usage = false;
SET query_cache_passive_usage = false;
EXPLAIN SELECT n from t;

-- try use cache when it is empty
SET query_cache_active_usage = false;
SET query_cache_passive_usage = true;
EXPLAIN SELECT n from t;

-- query result is put in cache but not accessed
SET query_cache_active_usage = true;
SET query_cache_passive_usage = false;
EXPLAIN SELECT n from t;

-- put query result in cache and access it in further queries
SET query_cache_active_usage = true;
SET query_cache_passive_usage = true;
SELECT n from t;
EXPLAIN SELECT n from t;
