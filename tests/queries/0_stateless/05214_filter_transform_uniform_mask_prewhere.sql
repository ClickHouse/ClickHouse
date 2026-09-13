SET enable_multiple_prewhere_read_steps = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET allow_reorder_prewhere_conditions = 0;

DROP TABLE IF EXISTS t_filter_transform_uniform_mask_prewhere;

CREATE TABLE t_filter_transform_uniform_mask_prewhere
(
    id UInt64,
    selective UInt8,
    all_true UInt8,
    all_false UInt8
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1000;

INSERT INTO t_filter_transform_uniform_mask_prewhere
SELECT
    number,
    toUInt8(number = 50),
    toUInt8(2),
    toUInt8(0)
FROM numbers(100);

SELECT count(), sum(id)
FROM t_filter_transform_uniform_mask_prewhere
PREWHERE selective AND all_true;

SELECT count(), sum(id)
FROM t_filter_transform_uniform_mask_prewhere
PREWHERE selective AND all_false;

SELECT count(), sum(id)
FROM t_filter_transform_uniform_mask_prewhere
PREWHERE all_true AND selective;

DROP TABLE t_filter_transform_uniform_mask_prewhere;
