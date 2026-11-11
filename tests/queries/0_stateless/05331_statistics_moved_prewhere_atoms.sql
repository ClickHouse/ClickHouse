-- A condition moved to PREWHERE stays in the filter above the read as an input column, and the
-- selectivity estimator counts it once, in the PREWHERE estimate. Without this the column counted
-- as an unknown predicate with the default factor on top, so the same conjunction estimated fewer
-- rows when part of it moved than when nothing moved.
--
-- The assertion compares the two estimates with each other, so it holds whatever the default
-- factors and the sketch precision are, and requires them to be positive so that a failed
-- extraction cannot satisfy it.
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 3;

DROP TABLE IF EXISTS t_moved;
CREATE TABLE t_moved (k UInt64 STATISTICS(uniq, basic), v UInt64 STATISTICS(uniq, basic), s String)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO t_moved SELECT number % 10000, number % 1000, toString(number) FROM numbers(100000);

-- With `move_all_conditions_to_prewhere = 0` only `v < 100` moves to PREWHERE; `k < 5000` and the
-- condition on `s` stay in the filter above the read.
SELECT 'with prewhere > 0:', with_prewhere > 0, 'equal:', with_prewhere = without_prewhere
FROM
(
    SELECT
        (
            SELECT toFloat64OrNull(extract(arrayStringConcat(groupArray(explain), '\n'), 'Filter[^\n]*rows: ~([0-9.]+)'))
            FROM (
                EXPLAIN estimates = 1 SELECT count() FROM t_moved WHERE k < 5000 AND v < 100 AND length(s) > 3
                SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, optimize_move_to_prewhere = 1, move_all_conditions_to_prewhere = 0
            )
        ) AS with_prewhere,
        (
            SELECT toFloat64OrNull(extract(arrayStringConcat(groupArray(explain), '\n'), 'Filter[^\n]*rows: ~([0-9.]+)'))
            FROM (
                EXPLAIN estimates = 1 SELECT count() FROM t_moved WHERE k < 5000 AND v < 100 AND length(s) > 3
                SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, optimize_move_to_prewhere = 0
            )
        ) AS without_prewhere
)
SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_moved;
