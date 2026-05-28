-- Issue #78166: WHERE over UNION where each branch wraps a const in materialize

SELECT uniq(id) FROM (
    SELECT 'online' AS event_type, 'i' AS id FROM numbers(100)
    UNION ALL
    SELECT 'click' AS event_type, '2' AS id FROM numbers(100)
) AS t
WHERE event_type = 'online';

-- both per-branch filters fold to constants
SELECT 'folded filters', countIf(explain LIKE '%Const(UInt8) -> equals%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT uniq(id) FROM (
        SELECT 'online' AS event_type, 'i' AS id FROM numbers(100)
        UNION ALL
        SELECT 'click' AS event_type, '2' AS id FROM numbers(100)
    ) AS t
    WHERE event_type = 'online'
);

-- explicit materialize(const) without UNION
SELECT count() FROM numbers(100)
WHERE materialize('online'::String) = 'online';

SELECT 'simple folded', countIf(explain LIKE '%Const(UInt8) -> equals%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM numbers(100)
    WHERE materialize('online'::String) = 'online'
);

SELECT count() FROM numbers(100) WHERE isConstant(materialize('online'));
