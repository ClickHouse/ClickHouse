-- a fixed granularity part attached to a table with adaptive granularity gets an adaptive projection part
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_fixed;
DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_fixed (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_real AS t_est;

INSERT INTO t_fixed SELECT number, number % 100, number FROM numbers(1000);
ALTER TABLE t_est ATTACH PARTITION tuple() FROM t_fixed;
ALTER TABLE t_real ATTACH PARTITION tuple() FROM t_fixed;
ALTER TABLE t_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
ALTER TABLE t_real MATERIALIZE PROJECTION p_b SETTINGS mutations_sync = 2;

CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, v ORDER BY b);

SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE b < 50)
WHERE match(trim(explain), '^(status|verdict):');
SELECT if(explain LIKE '%ReadFromMergeTree (p_b)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, v FROM t_real WHERE b < 50) WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE t_fixed;
DROP TABLE t_est;
DROP TABLE t_real;
