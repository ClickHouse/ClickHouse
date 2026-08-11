-- Tests advanced statistics estimator 5A: column-to-column comparisons.

SET explain_query_plan_default = 'legacy';
SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET materialize_statistics_on_insert = 1;
SET allow_reorder_prewhere_conditions = 1;
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS test_statistics_column_comparison_estimator;

CREATE TABLE test_statistics_column_comparison_estimator
(
    id UInt64,
    cc_high Int64 STATISTICS(basic),
    cc_low Int64 STATISTICS(basic),
    cc_probe Int64 STATISTICS(basic, uniq),
    cc_unique Int64 STATISTICS(basic, uniq),
    cc_mod Int64 STATISTICS(basic, uniq),
    cc_nan Float64 STATISTICS(basic),
    cc_na Nullable(Int64) STATISTICS(basic),
    cc_nb Nullable(Int64) STATISTICS(basic)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '', default_compression_codec = 'LZ4';

INSERT INTO test_statistics_column_comparison_estimator
SELECT
    number,
    200 + (number % 100),
    number % 100,
    number % 100,
    toInt64(number),
    number % 100,
    nan,
    if(number % 5 = 0, NULL, number % 100),
    if(number % 10 = 0, NULL, number % 100)
FROM numbers(10000);

SELECT '-- 5A column-to-column comparison estimates';

-- cc_high < cc_low is proven impossible from min/max and should be ordered
-- before the scalar equality probe.
SELECT position(prewhere_line, 'cc_high') > 0 AND position(prewhere_line, 'cc_probe') > 0 AND position(prewhere_line, 'cc_high') < position(prewhere_line, 'cc_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_column_comparison_estimator
        WHERE cc_probe = 1 AND cc_high < cc_low
    )
    WHERE explain LIKE '%Prewhere filter column%'
);

-- Column-to-column equality uses real NDV statistics and should be ordered
-- before a broad scalar range probe.
SELECT position(prewhere_line, 'cc_unique') > 0 AND position(prewhere_line, 'cc_probe') > 0 AND position(prewhere_line, 'cc_unique') < position(prewhere_line, 'cc_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_column_comparison_estimator
        WHERE cc_probe < 20 AND cc_unique = cc_mod
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_column_comparison_estimator WHERE cc_unique = cc_mod;

-- Float same-column equality can be false for NaN values, so it must not use
-- the exact same-column shortcut.
SELECT position(prewhere_line, 'cc_nan') > 0 AND position(prewhere_line, 'cc_probe') > 0 AND position(prewhere_line, 'cc_nan') < position(prewhere_line, 'cc_probe') FROM
(
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line
    FROM
    (
        EXPLAIN actions = 1
        SELECT count() FROM test_statistics_column_comparison_estimator
        WHERE cc_probe < 50 AND cc_nan = cc_nan
    )
    WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_statistics_column_comparison_estimator WHERE cc_nan = cc_nan;

-- Nullable column comparisons combined with NULL checks must keep SQL three-valued logic.
SELECT count() FROM test_statistics_column_comparison_estimator WHERE cc_na < cc_nb AND cc_na IS NULL;
SELECT count() FROM test_statistics_column_comparison_estimator WHERE cc_na <= cc_na OR cc_na IS NULL;

DROP TABLE test_statistics_column_comparison_estimator;
