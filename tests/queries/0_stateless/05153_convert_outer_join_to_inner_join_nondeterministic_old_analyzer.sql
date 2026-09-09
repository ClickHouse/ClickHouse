-- The guard under test lives in the legacy `JoinStep` branch of `convertOuterJoinToInnerJoin`, which
-- only the old analyzer reaches, and the runner never randomizes `enable_analyzer` away from a
-- session-level `SET`.
SET enable_analyzer = 0;

SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0;
SET join_algorithm = 'hash';
-- The conversion runs on `JoinStep` only when the join is not spilling, and the plan shape asserted
-- below must not depend on the global spill defaults.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (id UInt64, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left VALUES (1, 'Value_1'), (2, 'Value_2');
INSERT INTO t_right VALUES (2, 'Value_2'), (3, 'Value_3');

-- CONTROL: the deterministic predicate is false for the default right-side row, so the `LEFT JOIN`
-- still becomes an `INNER JOIN`. Without this row a red on the next one could just mean that the
-- conversion stopped happening at all.
SELECT 'deterministic filter', trimLeft(explain)
FROM
(
    EXPLAIN actions = 1 SELECT t_left.id FROM t_left LEFT JOIN t_right ON t_left.id = t_right.id
    WHERE t_right.id != 0
    SETTINGS query_plan_convert_outer_join_to_inner_join = 1, query_plan_join_swap_table = 'false',
        query_plan_merge_filter_into_join_condition = 0, enable_join_runtime_filters = 0
)
WHERE explain LIKE '%  Type: %';

-- A filter drawn per row observes which rows the join emits, so converting the join changes its
-- per-row results even though the filter would drop the not-matched rows anyway. The join must stay
-- `LEFT`.
SELECT 'non deterministic filter', trimLeft(explain)
FROM
(
    EXPLAIN actions = 1 SELECT t_left.id FROM t_left LEFT JOIN t_right ON t_left.id = t_right.id
    WHERE t_right.id != 0 AND rand(t_left.id) % 2 = 0
    SETTINGS query_plan_convert_outer_join_to_inner_join = 1, query_plan_join_swap_table = 'false',
        query_plan_merge_filter_into_join_condition = 0, enable_join_runtime_filters = 0
)
WHERE explain LIKE '%  Type: %';

DROP TABLE t_left;
DROP TABLE t_right;
