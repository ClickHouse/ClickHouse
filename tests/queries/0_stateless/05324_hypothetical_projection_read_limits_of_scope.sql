-- the projection scan obeys the read limits of the subquery that reads the table, as the read does

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_scope;
CREATE TABLE t_scope (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
INSERT INTO t_scope SELECT number, number % 100, number FROM numbers(1000);

CREATE HYPOTHETICAL PROJECTION p_b ON t_scope (SELECT a, b, v ORDER BY b);

-- the read of the subquery takes 1 mark, the scan of the whole part takes 1000 rows
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT * FROM (SELECT a, b, v FROM t_scope WHERE a = 42 AND b = 42 SETTINGS max_rows_to_read = 500))
WHERE match(trim(explain), '^(status|empirical_status|empirical_reason):');

DROP TABLE t_scope;
