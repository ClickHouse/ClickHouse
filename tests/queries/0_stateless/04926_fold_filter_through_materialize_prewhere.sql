-- Tags: no-old-analyzer
-- The `PREWHERE` fold needs the `ReadFromMergeTree` step to carry the `PREWHERE` expression as a
-- query plan `ActionsDAG` whose outputs get pruned; the old analyzer builds `PREWHERE` before the
-- plan and never reaches that pruning, so the constant is not folded there.
-- The `PREWHERE` fold happens while `ReadFromMergeTree` prunes the outputs of the `PREWHERE`
-- expression, which is part of the `removeUnusedColumns` query plan optimization, so pin it
-- (the test runner randomizes it).
SET query_plan_remove_unused_columns = 1;
-- Only the pretty plan renders the folded constant; the legacy one keeps the original expression name.
SET explain_query_plan_default = 'pretty';

CREATE TABLE prewhere_materialize_fold (id UInt64)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO prewhere_materialize_fold SELECT number FROM numbers(10);

-- `EXPLAIN` renders the filter expression, and its indentation and inner spacing are not part of
-- what is asserted here, so collapse every whitespace run before matching.
SELECT 'folded PREWHERE filter', countIf(replaceRegexpAll(explain, '\\s+', ' ') LIKE '%Prewhere filter column: 0')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT count()
    FROM prewhere_materialize_fold
    PREWHERE materialize(1) = 0
);

SELECT count()
FROM prewhere_materialize_fold
PREWHERE materialize(1) = 0;

DROP TABLE prewhere_materialize_fold;
