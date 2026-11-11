-- A condition the statistics cannot estimate, such as a function of a sorting key column, used to take a
-- flat default (one percent for `IN`, a third for a range). The primary key analysis already selected
-- the granules such a condition keeps: their share of the table now stands for it, and the other
-- conditions keep their statistics. The table has 196 rows per day, in granules of 1024 rows.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
-- The plan lines below show the filters moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
-- The plan lines below show one filter expression per read; the runner randomizes the merge.
SET query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_dk_fact;
DROP TABLE IF EXISTS t_dk_dim;

-- The granule count decides the share; the runner randomizes the granularity and the sparse serialization.
CREATE TABLE t_dk_fact (order_date Date, v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY order_date
    SETTINGS auto_statistics_types = '', index_granularity = 1024, index_granularity_bytes = '10Mi',
        ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_dk_dim (v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_dk_fact SELECT toDate('2023-01-01') + intDiv(number, 196), number % 100 FROM numbers(196 * 730);
INSERT INTO t_dk_dim SELECT number FROM numbers(100);
OPTIMIZE TABLE t_dk_fact FINAL;

SELECT '-- the quarter of the sorting key IN two quarters: about a quarter of the table';
SELECT count() FROM t_dk_fact WHERE toStartOfQuarter(order_date) IN ('2023-04-01', '2024-01-01');
EXPLAIN estimates = 1 SELECT count() FROM t_dk_fact AS f JOIN t_dk_dim AS d ON f.v = d.v
WHERE toStartOfQuarter(f.order_date) IN ('2023-04-01', '2024-01-01');

SELECT '-- the same with a condition the statistics estimate: both count';
SELECT count() FROM t_dk_fact WHERE toStartOfQuarter(order_date) IN ('2023-04-01', '2024-01-01') AND v < 10;
EXPLAIN estimates = 1 SELECT count() FROM t_dk_fact AS f JOIN t_dk_dim AS d ON f.v = d.v
WHERE toStartOfQuarter(f.order_date) IN ('2023-04-01', '2024-01-01') AND f.v < 10;

SELECT '-- a range over a conversion of the sorting key: half a year';
SELECT count() FROM t_dk_fact WHERE toDate(order_date) BETWEEN '2023-03-01' AND '2023-08-31';
EXPLAIN estimates = 1 SELECT count() FROM t_dk_fact AS f JOIN t_dk_dim AS d ON f.v = d.v
WHERE toDate(f.order_date) BETWEEN '2023-03-01' AND '2023-08-31';

SELECT '-- a range on the sorting key column itself, which has no statistics here: the key analysis measures it too';
EXPLAIN estimates = 1 SELECT count() FROM t_dk_fact AS f JOIN t_dk_dim AS d ON f.v = d.v
WHERE f.order_date BETWEEN '2023-03-01' AND '2023-08-31';

SELECT '-- a condition the key analysis represents only in part keeps the statistics: no share replaces it';
SELECT count() FROM t_dk_fact WHERE (toStartOfQuarter(order_date) = '2023-04-01' AND v = 1) OR (toStartOfQuarter(order_date) = '2024-01-01' AND v = 2);
EXPLAIN estimates = 1 SELECT count() FROM t_dk_fact AS f JOIN t_dk_dim AS d ON f.v = d.v
WHERE (toStartOfQuarter(f.order_date) = '2023-04-01' AND f.v = 1) OR (toStartOfQuarter(f.order_date) = '2024-01-01' AND f.v = 2);

-- Partition pruning removes whole parts before the statistics see them: the share is taken over the
-- parts left, or the pruning would count twice.
DROP TABLE IF EXISTS t_dk_parts;
CREATE TABLE t_dk_parts (order_date Date, v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree PARTITION BY toYear(order_date) ORDER BY order_date
    SETTINGS auto_statistics_types = '', index_granularity = 1024, index_granularity_bytes = '10Mi',
        ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_dk_parts SELECT toDate('2023-01-01') + intDiv(number, 196), number % 100 FROM numbers(196 * 730);
OPTIMIZE TABLE t_dk_parts FINAL;

SELECT '-- partitions by year, a quarter of 2023 and a quarter of 2024';
SELECT count() FROM t_dk_parts WHERE toYear(order_date) = 2023 AND toStartOfQuarter(order_date) IN ('2023-04-01', '2024-01-01');
EXPLAIN estimates = 1 SELECT count() FROM t_dk_parts AS f JOIN t_dk_dim AS d ON f.v = d.v
WHERE toYear(f.order_date) = 2023 AND toStartOfQuarter(f.order_date) IN ('2023-04-01', '2024-01-01');

DROP TABLE t_dk_parts;
DROP TABLE t_dk_fact;
DROP TABLE t_dk_dim;
