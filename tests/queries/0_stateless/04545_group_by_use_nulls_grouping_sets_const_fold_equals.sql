-- Tags: no-old-analyzer
-- Regression for https://github.com/ClickHouse/ClickHouse/issues/110551
-- group_by_use_nulls=1 promotes a constant-folded comparison to Nullable via
-- FunctionNode::wrap_with_nullable (a QueryTree/analyzer-only mechanism), while the
-- un-wrapped base folded a Const(UInt8). The ActionsDAG node result-type check aborted
-- debug/sanitizer builds. no-old-analyzer: the fold path is analyzer-era only.
SELECT * FROM
(
    SELECT DISTINCT
        isDistinctFrom(-2147483647, NULL) AS a,
        currentDatabase() = database AS b,
        indexHint((currentDatabase() = database) AND 256, toFixedString(2, NULL)) AS c,
        toString(2147483646) AS d
    GROUP BY GROUPING SETS
        ((toLowCardinality(-9223372036854775807)),
         (materialize(toLowCardinality(toInt128(9223372036854775806))) >= 2147483648),
         (2))
    HAVING -1
    SETTINGS group_by_use_nulls = 1
)
ORDER BY a, b NULLS LAST, c, d;

SELECT number, 1 = 1 AS eq, count()
FROM numbers(3)
GROUP BY GROUPING SETS ((number), (1 = 1))
ORDER BY number NULLS LAST, eq
SETTINGS group_by_use_nulls = 1;
