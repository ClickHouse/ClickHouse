-- Tags: no-tsan
-- The nesting depth below is what makes the un-memoized fold explode, but it also makes query
-- analysis recurse deeply, and under TSan only 5% of the thread stack may be used
-- (`checkStackSize`), which the analyzer exhausts before the plan is ever built.
-- The predicate fold walks the filter DAG after `deduplicateSubtrees`, so every level below shares
-- one node with two incoming edges. Without memoization the walk costs one visit per edge, which is
-- 2^26 folds here and takes minutes; it must stay instant.
SELECT count() FROM
(
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
    SELECT (x AND x) AS x FROM
    (
SELECT materialize(1) = 1 AS x FROM numbers(10)
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
    )
)
WHERE x
SETTINGS max_execution_time = 30;
