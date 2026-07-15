-- The imprecise estimate makes join reordering log a diagnostic message; suppress server logs from
-- the client log stream so the test harness does not treat it as unexpected stderr output.
SET send_logs_level = 'fatal';

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET allow_experimental_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_no_stats_a;
DROP TABLE IF EXISTS t_no_stats_b;
DROP TABLE IF EXISTS t_stats_a;
DROP TABLE IF EXISTS t_stats_b;

-- `auto_statistics_types` defaults to a non-empty value, so MergeTree tables get column statistics
-- automatically. Disable it for these tables to model the case where no statistics are available.
CREATE TABLE t_no_stats_a (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE t_no_stats_b (id UInt64, y UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

CREATE TABLE t_stats_a (id UInt64 STATISTICS(uniq), x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_stats_b (id UInt64 STATISTICS(uniq), y UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_no_stats_a SELECT number, number FROM numbers(1000);
INSERT INTO t_no_stats_b SELECT number, number FROM numbers(100);
INSERT INTO t_stats_a SELECT number, number FROM numbers(1000);
INSERT INTO t_stats_b SELECT number, number FROM numbers(100);

OPTIMIZE TABLE t_no_stats_a FINAL;
OPTIMIZE TABLE t_no_stats_b FINAL;
OPTIMIZE TABLE t_stats_a FINAL;
OPTIMIZE TABLE t_stats_b FINAL;

SET use_statistics = 1;

SELECT '-- no column statistics, use_statistics=1: estimation is imprecise --';
SELECT trimLeft(explain) FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM t_no_stats_a a JOIN t_no_stats_b b ON a.id = b.id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- column statistics present, use_statistics=1: estimation is precise (no label) --';
SELECT trimLeft(explain) FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM t_stats_a a JOIN t_stats_b b ON a.id = b.id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- mixed: one side without statistics: top join is imprecise --';
SELECT trimLeft(explain) FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM t_stats_a a JOIN t_no_stats_b b ON a.id = b.id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SET use_statistics = 0;

SELECT '-- use_statistics=0: index estimate is still labeled, it is not derived from column statistics --';
SELECT trimLeft(explain) FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT * FROM t_no_stats_a a JOIN t_no_stats_b b ON a.id = b.id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_no_stats_a;
DROP TABLE t_no_stats_b;
DROP TABLE t_stats_a;
DROP TABLE t_stats_b;
