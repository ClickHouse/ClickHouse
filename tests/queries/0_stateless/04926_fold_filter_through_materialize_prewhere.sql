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
