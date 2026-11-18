-- the writer sizes the granules of a projection part with the columns of its key expression, also when the part does not store them
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, v ORDER BY intDiv(b, 10));

INSERT INTO t_est SELECT number, number % 100, number FROM numbers(3000);
INSERT INTO t_real SELECT number, number % 100, number FROM numbers(3000);

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, v ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE intDiv(b, 10) = 4)
WHERE match(line, '^(Baseline|With |marks:|status:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real WHERE intDiv(b, 10) = 4)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

DROP TABLE t_est;
DROP TABLE t_real;
