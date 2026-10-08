-- Regression test: `IN` with a single literal value must not throw
-- "Bad get: has String, requested Tuple" in ConditionSelectivityEstimator.

SET allow_experimental_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 0;
SET query_plan_optimize_prewhere = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    category String STATISTICS(uniq),
    val      UInt64
)
ENGINE = MergeTree
ORDER BY val;

INSERT INTO tab SELECT if(number % 2 = 0, 'alice', 'bob'), number FROM numbers(100);
OPTIMIZE TABLE tab FINAL;

-- Single literal: before the fix this threw BAD_GET.
SELECT count() FROM tab WHERE category IN ('alice');
SELECT count() FROM tab WHERE category IN ('carol');

DROP TABLE tab;
