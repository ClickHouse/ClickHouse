-- A relation whose row estimate is unknown is costed by the join order optimizer at the rows it
-- cannot exceed (the rows the read selects), so a missing estimate does not make a plan look cheap;
-- without this it counted as one row and was joined first. The hints stand in for column
-- statistics of the known tables; `t_unk` has no statistics and a filter the primary index cannot
-- use, so its estimate is unknown.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
-- Runtime filters add steps to the plan, and the hash table statistics of an executed join would
-- replace a relation's estimate in the next plan.
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET param__internal_join_table_stat_hints = '{"t_big": {"cardinality": 10000, "distinct_keys": {"k": 10000}}, "t_small": {"cardinality": 100, "distinct_keys": {"k": 100}}, "t_empty": {"cardinality": 0, "distinct_keys": {"k": 1}}}';

DROP TABLE IF EXISTS t_big;
DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_unk;
DROP TABLE IF EXISTS t_empty;

CREATE TABLE t_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_small (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_unk (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_empty (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_big SELECT number, number FROM numbers(10000);
INSERT INTO t_small SELECT number * 100, number FROM numbers(100);
INSERT INTO t_unk SELECT number * 10, number FROM numbers(1000);
-- One row keeps the read a MergeTree read, so the hint with cardinality 0 applies.
INSERT INTO t_empty VALUES (1, 1);

-- The unknown relation is joined last: its cost counts the 1000 selected rows, not one row (110 instead of 100).
SELECT '-- unknown relation costed by its bound';
EXPLAIN estimates = 1
SELECT count()
FROM t_big AS b
JOIN t_small AS s ON b.k = s.k
JOIN t_unk AS u ON b.k = u.k
WHERE u.v < 5;

-- The join with an unknown input is counted.
SELECT count()
FROM t_big AS b
JOIN t_small AS s ON b.k = s.k
JOIN t_unk AS u ON b.k = u.k
WHERE u.v < 5
SETTINGS log_comment = '05327_unknown_rows_cost';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['JoinOrderJoinsCostedWithoutRowEstimate']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
  AND log_comment = '05327_unknown_rows_cost' AND type = 'QueryFinish';

-- A full join emits every row of both sides at least once: at least the larger side, not the sum.
SELECT '-- full join floor';
EXPLAIN estimates = 1
SELECT count()
FROM t_big AS b
FULL JOIN t_small AS s ON b.k = s.k;

-- An input known to be empty makes an inner join empty, not one row.
SELECT '-- empty input';
EXPLAIN estimates = 1
SELECT count()
FROM t_big AS b
JOIN t_empty AS e ON b.k = e.k;

-- The DPsub algorithm sees the relation estimates too: its plan is the greedy one above.
SELECT '-- dpsub agrees with greedy',
    (SELECT groupArray(explain) FROM (EXPLAIN estimates = 1
        SELECT count() FROM t_big AS b JOIN t_small AS s ON b.k = s.k JOIN t_unk AS u ON b.k = u.k WHERE u.v < 5
        SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub'))
    = (SELECT groupArray(explain) FROM (EXPLAIN estimates = 1
        SELECT count() FROM t_big AS b JOIN t_small AS s ON b.k = s.k JOIN t_unk AS u ON b.k = u.k WHERE u.v < 5
        SETTINGS query_plan_optimize_join_order_algorithm = 'greedy'));

DROP TABLE t_big;
DROP TABLE t_small;
DROP TABLE t_unk;
DROP TABLE t_empty;
