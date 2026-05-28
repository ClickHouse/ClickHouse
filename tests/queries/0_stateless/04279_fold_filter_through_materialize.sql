-- Issue #78166: WHERE over UNION where each branch wraps a const in materialize

-- Original issue: predicate must fold per-branch and the always-false branch is pruned
SELECT uniq(id) FROM (
    SELECT 'online' AS event_type, 'i' AS id FROM numbers(100)
    UNION ALL
    SELECT 'click' AS event_type, '2' AS id FROM numbers(100)
) AS t
WHERE event_type = 'online';

-- one branch folds to true (kept) and the other to false (replaced with ReadNothing)
SELECT 'fold + prune',
       countIf(explain LIKE '%Const(UInt8) -> equals%'),
       countIf(explain LIKE '%ReadNothing%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT uniq(id) FROM (
        SELECT 'online' AS event_type, 'i' AS id FROM numbers(100)
        UNION ALL
        SELECT 'click' AS event_type, '2' AS id FROM numbers(100)
    ) AS t
    WHERE event_type = 'online'
);

-- both branches are pure consts comapared with a value - whole input is pruned
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 3 ORDER BY x;
SELECT 'both pruned', countIf(explain LIKE '%ReadNothing%')
FROM (
    EXPLAIN PLAN
    SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 3
);

-- only one branch passes
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 1 ORDER BY x;

-- standalone WHERE materialize(const) = const
SELECT count() FROM numbers(100) WHERE materialize('online'::String) = 'online';
SELECT 'simple folded', countIf(explain LIKE '%Const(UInt8) -> equals%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM numbers(100) WHERE materialize('online'::String) = 'online'
);

-- nested materialize chain - resolveConstThroughMaterialize must walk through both
SELECT count() FROM numbers(100) WHERE materialize(materialize('online'::String)) = 'online';

SELECT count() FROM numbers(100) WHERE materialize(CAST(NULL AS Nullable(UInt8)));

SELECT count() FROM numbers(100) WHERE isConstant(materialize('online'));

SELECT count() FROM numbers(10) WHERE materialize(now()) > toDateTime('1970-01-01');
