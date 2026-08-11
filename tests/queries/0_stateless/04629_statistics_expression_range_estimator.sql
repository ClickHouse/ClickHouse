-- Tests advanced statistics estimator 5B: expression-derived ranges.

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET materialize_statistics_on_insert = 1;
SET allow_reorder_prewhere_conditions = 1;
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS test_statistics_expression_range_estimator;

CREATE TABLE test_statistics_expression_range_estimator
(
    id UInt64,
    expr_a Int32 STATISTICS(basic),
    expr_probe Int32 STATISTICS(basic),
    expr_big Int64 STATISTICS(basic)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '', default_compression_codec = 'LZ4';

INSERT INTO test_statistics_expression_range_estimator
SELECT
    number,
    number % 10000,
    number % 10000,
    toInt64(9223372036854775807) - toInt64(number % 2)
FROM numbers(10000);

SELECT '-- 5B expression-derived range estimates';

-- Safe widening casts are normalized to the underlying column range, making
-- expr_a < 10 more selective than expr_probe < 1000.
SELECT position(prewhere_line, 'expr_a') > 0 AND position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_a') < position(prewhere_line, 'expr_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND CAST(expr_a, 'Int64') < 10
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

-- Equality comparisons through safe widening casts are normalized as well.
SELECT position(prewhere_line, 'expr_a') > 0 AND position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_a') < position(prewhere_line, 'expr_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND CAST(expr_a, 'Int64') = 42
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_expression_range_estimator WHERE CAST(expr_a, 'Int64') = 42;

-- Checked arithmetic rewrites are likewise normalized when the column range
-- proves the expression cannot overflow.
SELECT position(prewhere_line, 'expr_a') > 0 AND position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_a') < position(prewhere_line, 'expr_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND expr_a + 1 < 10
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

-- Constant-left plus is equivalent and should normalize through the same path.
SELECT position(prewhere_line, 'expr_a') > 0 AND position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_a') < position(prewhere_line, 'expr_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND 1 + expr_a < 10
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_expression_range_estimator WHERE 1 + expr_a < 10;

-- Subtraction by a constant is normalized by shifting the comparison bound.
SELECT position(prewhere_line, 'expr_a') > 0 AND position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_a') < position(prewhere_line, 'expr_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND expr_a - 1 < 10
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_expression_range_estimator WHERE expr_a - 1 < 10;

-- Constant-on-left comparisons are inverted before expression normalization.
SELECT position(prewhere_line, 'expr_a') > 0 AND position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_a') < position(prewhere_line, 'expr_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND 10 > expr_a + 1
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_expression_range_estimator WHERE 10 > expr_a + 1;

-- Arithmetic that can overflow for values present in the column falls back, so
-- the ordinary expr_probe range remains the better prewhere predicate.
SELECT position(prewhere_line, 'expr_probe') > 0 AND position(prewhere_line, 'expr_big') > 0 AND position(prewhere_line, 'expr_probe') < position(prewhere_line, 'expr_big') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_expression_range_estimator
        WHERE expr_probe < 1000 AND expr_big + 1 < 0
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

SELECT count() FROM test_statistics_expression_range_estimator WHERE CAST(expr_a, 'Int64') < 10;
SELECT count() FROM test_statistics_expression_range_estimator WHERE expr_a + 1 < 10;

DROP TABLE test_statistics_expression_range_estimator;
