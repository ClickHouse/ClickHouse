-- Without column stats the optimizer must still pick the smaller side as the
-- hash-join build, and expose the result-row estimate as `unknown` so untrusted
-- fallback numbers cannot leak past the trust boundary to upstream joins.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_fact_04103;
DROP TABLE IF EXISTS t_dim_04103;

CREATE TABLE t_fact_04103 (k UInt64) ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_dim_04103 (k UInt64) ORDER BY k SETTINGS auto_statistics_types = '';

INSERT INTO t_fact_04103 SELECT number % 100 FROM numbers(1000000);
INSERT INTO t_dim_04103 SELECT number FROM numbers(100);

-- Expected: 100-row dimension is on the build side; ResultRows: unknown.
SELECT trimBoth(explain) FROM (
    EXPLAIN actions=1, keep_logical_steps=1
    SELECT count() FROM t_fact_04103 f JOIN t_dim_04103 d ON f.k = d.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_fact_04103;
DROP TABLE t_dim_04103;
