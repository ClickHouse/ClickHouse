-- the plans that EXPLAIN WHATIF makes for one statement run an IN subquery once

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0, log_queries = 1;

DROP TABLE IF EXISTS t_keys;
DROP TABLE IF EXISTS t_facts;
CREATE TABLE t_keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_keys SELECT number FROM numbers(50);
CREATE TABLE t_facts (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
INSERT INTO t_facts SELECT number, number % 100, number FROM numbers(3000);

CREATE HYPOTHETICAL PROJECTION p_b ON t_facts (SELECT a, b, v ORDER BY b);

-- the subquery sleeps 1 ms for each of its 50 rows, so each run adds 50000 microseconds
SELECT count() FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_facts WHERE a < 2000 AND b IN (SELECT k FROM t_keys WHERE sleepEachRow(0.001) = 0))
SETTINGS log_comment = '05338_in_subquery_runs_once' FORMAT Null;

-- with use_index_for_in_with_subqueries_max_values the set has no explicit elements, and a new build gives the same set
SELECT count() FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_facts WHERE a < 2000 AND b IN (SELECT k FROM t_keys WHERE sleepEachRow(0.001) = 0))
SETTINGS use_index_for_in_with_subqueries_max_values = 10, log_comment = '05338_in_subquery_runs_once_max_values' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SleepFunctionMicroseconds'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '05338_in_subquery_runs_once%' AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_keys;
DROP TABLE t_facts;
