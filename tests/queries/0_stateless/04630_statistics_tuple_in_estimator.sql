-- Tests advanced statistics estimator 5C: tuple and multi-column IN.

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET materialize_statistics_on_insert = 1;
SET allow_reorder_prewhere_conditions = 1;
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS test_statistics_tuple_in_estimator;

CREATE TABLE test_statistics_tuple_in_estimator
(
    id UInt64,
    tin_a Int32 STATISTICS(basic, uniq),
    tin_b Int32 STATISTICS(basic, uniq),
    tin_probe Int32 STATISTICS(basic)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '', default_compression_codec = 'LZ4';

INSERT INTO test_statistics_tuple_in_estimator
SELECT
    number,
    number % 10,
    intDiv(number, 10) % 10,
    number % 10000
FROM numbers(10000);

SELECT '-- 5C tuple IN estimates';

-- Two tuple alternatives estimate 200 rows directly. If tuple alternatives were
-- merged into per-column ranges, the estimate would admit four combinations and
-- become less selective than tin_probe < 300.
SELECT position(prewhere_line, 'tin_a') > 0 AND position(prewhere_line, 'tin_probe') > 0 AND position(prewhere_line, 'tin_a') < position(prewhere_line, 'tin_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_tuple_in_estimator
        WHERE tin_probe < 300 AND (tin_a, tin_b) IN ((1, 2), (3, 4))
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

-- Five tuple alternatives estimate 500 rows, proving the tuple predicate did
-- not fall back to the old 1% unknown-expression estimate.
SELECT position(prewhere_line, 'tin_probe') > 0 AND position(prewhere_line, 'tin_a') > 0 AND position(prewhere_line, 'tin_probe') < position(prewhere_line, 'tin_a') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_tuple_in_estimator
        WHERE tin_probe < 300 AND (tin_a, tin_b) IN ((0, 0), (1, 1), (2, 2), (3, 3), (4, 4))
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

-- Negative tuple predicates use the complement of the distinct tuple alternatives.
SELECT position(prewhere_line, 'notIn') > 0 AND position(prewhere_line, 'tin_probe') > 0 AND position(prewhere_line, 'notIn') < position(prewhere_line, 'tin_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_tuple_in_estimator
        WHERE tin_probe < 9850 AND (tin_a, tin_b) NOT IN ((1, 2), (3, 4))
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_tuple_in_estimator WHERE (tin_a, tin_b) NOT IN ((1, 2), (3, 4));

-- Scalar predicates on tuple columns are correlated with tuple alternatives.
-- After tin_a = 3, only one tuple alternative survives, so tin_probe < 50 is
-- the more selective predicate.
SELECT position(prewhere_line, 'tin_probe') > 0 AND position(prewhere_line, 'tin_a') > 0 AND position(prewhere_line, 'tin_probe') < position(prewhere_line, 'tin_a') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_tuple_in_estimator
        WHERE tin_probe < 50 AND (tin_a, tin_b) IN ((1, 2), (3, 4)) AND tin_a = 3
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_tuple_in_estimator WHERE tin_probe < 50 AND (tin_a, tin_b) IN ((1, 2), (3, 4)) AND tin_a = 3;

-- Repeated LHS tuple columns are correlated: conflicting constants are
-- impossible, while equal constants reduce to one scalar equality.
SELECT count() FROM test_statistics_tuple_in_estimator WHERE (tin_a, tin_a) = (1, 1);
SELECT count() FROM test_statistics_tuple_in_estimator WHERE (tin_a, tin_a) = (1, 2);
SELECT count() FROM test_statistics_tuple_in_estimator WHERE (tin_a, tin_a) NOT IN ((1, 2));

DROP TABLE test_statistics_tuple_in_estimator;
