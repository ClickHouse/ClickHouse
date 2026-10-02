-- a projection that the optimizer rejects for the shape of the query is not applicable before any data is read
SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS no_filter;
CREATE TABLE no_filter (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO no_filter SELECT number, number % 100, number FROM numbers(100000);
CREATE HYPOTHETICAL PROJECTION p_b ON no_filter (SELECT a, b, v ORDER BY b);

SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM no_filter)
WHERE match(line, '^status:')
SETTINGS log_comment = '05321_no_filter';

SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 1000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05321_no_filter'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE no_filter;
