-- A row estimate that took a default selectivity, here the `LIKE` default of the statistics
-- estimator, is a guess. The replication byte budget does not take it on its own: the side has to
-- fit with its proven row bound, the rows the read selects. A side estimated from statistics alone
-- passes on its estimate.
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_default_reader_bucket_count = 2;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_db_big;
DROP TABLE IF EXISTS t_db_dim;
CREATE TABLE t_db_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_db_dim (k UInt64, name String STATISTICS(uniq, basic), grp UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_db_big SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_db_dim SELECT number, concat('name_', toString(number % 100)), number % 10 FROM numbers(1000);

-- The dimension side ships only the join key `k`, 8 bytes per row: the filter column is read and
-- dropped below the join. Each budget below fits the estimate of 100 rows (800 bytes) and not the
-- bound of 1000 rows (8000 bytes).
-- The read itself is distributed first: every node scanning the 1000 rows would exceed the budget.
SELECT '-- an equality estimated from statistics (100 of 1000 rows) fits the budget: broadcast';
EXPLAIN SELECT count() FROM t_db_big AS b JOIN t_db_dim AS d ON b.k = d.k WHERE d.grp = 3
SETTINGS distributed_plan_max_bytes_to_broadcast = 4000;

SELECT '-- the LIKE default also says 100 rows, but it is a guess and the bound of 1000 rows does not fit: shuffle';
EXPLAIN SELECT count() FROM t_db_big AS b JOIN t_db_dim AS d ON b.k = d.k WHERE d.name LIKE '%_3%'
SETTINGS distributed_plan_max_bytes_to_broadcast = 4000;

SELECT '-- with a budget the bound fits, the guess may broadcast';
EXPLAIN SELECT count() FROM t_db_big AS b JOIN t_db_dim AS d ON b.k = d.k WHERE d.name LIKE '%_3%'
SETTINGS distributed_plan_max_bytes_to_broadcast = 100000;

DROP TABLE t_db_big;
DROP TABLE t_db_dim;
