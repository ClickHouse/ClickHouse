DROP TABLE IF EXISTS t;

CREATE TABLE t (b UInt32) ENGINE=Memory;
INSERT INTO t VALUES (42);

SET query_cache_active_usage = true;
SET query_cache_passive_usage = true;
SET min_query_runs_before_caching = 5;

SET query_cache_entry_put_timeout_ms = 1000; -- 1 sec

SELECT b from t;
SELECT b from t;
SELECT b from t;
SELECT b from t;
SELECT b from t;

EXPLAIN SELECT b from t;

SELECT sleep(0.1);
EXPLAIN SELECT b from t;

SELECT sleep(0.2);
EXPLAIN SELECT b from t;

SELECT sleep(0.7);
EXPLAIN SELECT b from t;
