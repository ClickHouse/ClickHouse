-- Regression: LIMIT must not be pushed into the FINAL merge when the query uses
-- SAMPLE. The sampling FilterTransform is appended to the pipe *after* the FINAL
-- merge, so stopping the merge after N pre-sample groups can drop later sampled
-- keys that should fill the top-N.

DROP TABLE IF EXISTS t_final_sample;

CREATE TABLE t_final_sample
(
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (key, intHash32(key))
SAMPLE BY intHash32(key);

-- Two parts per key so that FINAL has something to merge.
INSERT INTO t_final_sample
    SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_sample
    SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

-- The result with pushdown enabled must be identical to the result with it
-- disabled: the gate must refuse to push the limit below the sampling filter.
SELECT 'sample_top5_pushdown_on_equals_off';
SELECT
    (
        SELECT groupArray((key, v)) FROM
        (
            SELECT key, finalizeAggregation(val) AS v
            FROM t_final_sample FINAL
            SAMPLE 1 / 2
            ORDER BY key ASC
            LIMIT 5
            SETTINGS optimize_final_limit_pushdown = 1
        )
    ) =
    (
        SELECT groupArray((key, v)) FROM
        (
            SELECT key, finalizeAggregation(val) AS v
            FROM t_final_sample FINAL
            SAMPLE 1 / 2
            ORDER BY key ASC
            LIMIT 5
            SETTINGS optimize_final_limit_pushdown = 0
        )
    );

-- With SAMPLE, no `Description: limit ...` must be attached to the merging
-- transform even when the optimization is enabled.
SELECT 'explain_sample_no_limit_in_merge';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_final_sample FINAL
    SAMPLE 1 / 2
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

-- Positive control: without SAMPLE the same query does push the limit down,
-- proving that the sampling check is what blocks it above.
SELECT 'explain_no_sample_limit_in_merge';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_final_sample FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

DROP TABLE t_final_sample;
