-- Tags: no-random-settings, no-random-merge-tree-settings
-- The `read_in_order_max_primary_key_ratio` guard is enforced only on the query-plan
-- read-in-order path (`query_plan_read_in_order = 1`, the default). The legacy planner
-- path (`query_plan_read_in_order = 0`) sets `input_order_info` directly via
-- `query_info.order_optimizer` and never consults the setting, so its plan must be
-- invariant to the ratio. This test pins that documented contract.

DROP TABLE IF EXISTS t_read_in_order_pk_qp_only;

-- Small index_granularity to produce enough marks for the guard to be able to fire
-- (it requires total_marks > requested_num_streams), and several parts to mirror the
-- motivating multi-part case.
CREATE TABLE t_read_in_order_pk_qp_only (path String, value UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_read_in_order_pk_qp_only;

INSERT INTO t_read_in_order_pk_qp_only SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO t_read_in_order_pk_qp_only SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO t_read_in_order_pk_qp_only SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);

SET max_threads = 4, enable_parallel_replicas = 0;

-- On the query-plan path, poor primary key selectivity with the ratio forced to 0.0 makes
-- the guard fire, so the `SortingStep` adds a full sort (`PartialSortingTransform`).
SELECT 'query_plan_guard_fires_on_poor_pk';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_qp_only
    WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS query_plan_read_in_order = 1, read_in_order_max_primary_key_ratio = 0.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- On the same query-plan path, ratio = 1.0 disables the guard, so read-in-order is kept
-- and there is no separate sort.
SELECT 'query_plan_setting_off_keeps_in_order';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_qp_only
    WHERE path LIKE '%file.log' ORDER BY path
    SETTINGS query_plan_read_in_order = 1, read_in_order_max_primary_key_ratio = 1.0
) WHERE explain LIKE '%PartialSortingTransform%';

-- On the legacy path the setting is not consulted, so the plan is identical regardless of
-- the ratio. We assert the `PartialSortingTransform` count is the same for ratio 0.0 and 1.0,
-- which would regress to differing values if the guard ever leaked into the legacy path.
SELECT 'legacy_path_ignores_setting';
SELECT
(
    SELECT count() FROM (
        EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_qp_only
        WHERE path LIKE '%file.log' ORDER BY path
        SETTINGS query_plan_read_in_order = 0, read_in_order_max_primary_key_ratio = 0.0
    ) WHERE explain LIKE '%PartialSortingTransform%'
)
=
(
    SELECT count() FROM (
        EXPLAIN PIPELINE SELECT * FROM t_read_in_order_pk_qp_only
        WHERE path LIKE '%file.log' ORDER BY path
        SETTINGS query_plan_read_in_order = 0, read_in_order_max_primary_key_ratio = 1.0
    ) WHERE explain LIKE '%PartialSortingTransform%'
);

DROP TABLE t_read_in_order_pk_qp_only;
