DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt32, b UInt32, c UInt32, d UInt32) ENGINE=Memory;
INSERT INTO t VALUES (0, 2, 4, 3);

SET query_cache_entry_put_timeout_ms = 1000050000;
SET query_cache_active_usage = true;
SET query_cache_passive_usage = true;

SET min_query_runs_before_caching = 1;
EXPLAIN SELECT a from t;

SET min_query_runs_before_caching = 2;
EXPLAIN SELECT b from t;
EXPLAIN SELECT b from t;

SET min_query_runs_before_caching = 3;
EXPLAIN SELECT c from t;
EXPLAIN SELECT c from t;
EXPLAIN SELECT c from t;

SET min_query_runs_before_caching = 4;
EXPLAIN SELECT d from t;
EXPLAIN SELECT d from t;
EXPLAIN SELECT d from t;
EXPLAIN SELECT d from t;
