-- when a time limit in `break` mode stops a sampled projection scan, the estimate must not use the partial rows
DROP TABLE IF EXISTS t_whatif_break;

CREATE TABLE t_whatif_break (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_whatif_break SELECT number, cityHash64(number) % 1000 FROM numbers(20000);

CREATE HYPOTHETICAL PROJECTION p_b ON t_whatif_break (SELECT a, b ORDER BY b);

-- the sample has 50 granules (5000 rows), and at 2000 rows/s the 1 s limit stops the read before the end
EXPLAIN WHATIF projection_scan_budget_rows = 5000 SELECT count() FROM t_whatif_break WHERE b < 100
SETTINGS optimize_trivial_count_query = 0, optimize_use_projections = 1,
    max_execution_speed = 2000, timeout_before_checking_execution_speed = 0,
    max_execution_time = 1, timeout_overflow_mode = 'break'
FORMAT Null;

SELECT 'ok';

DROP TABLE t_whatif_break;
