-- Issue #78166: WHERE over UNION where each branch wraps a const in materialize

SET enable_analyzer = 1;

SELECT uniq(id) FROM (
    SELECT 'online' AS event_type, 'i' AS id FROM numbers(100)
    UNION ALL
    SELECT 'click' AS event_type, '2' AS id FROM numbers(100)
) AS t
WHERE event_type = 'online';

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

-- pure const branches compared with a value - both branches fold
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 3 ORDER BY x;
SELECT 'both folded', countIf(explain LIKE '%Const(UInt8) -> greater%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 3
);

-- one branch passes, one rejects
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 1 ORDER BY x;

-- standalone WHERE materialize(const) = const
SELECT count() FROM numbers(100) WHERE materialize('online'::String) = 'online';
SELECT 'simple folded', countIf(explain LIKE '%Const(UInt8) -> equals%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM numbers(100) WHERE materialize('online'::String) = 'online'
);

-- nested materialize - the resolver walks through both wrappers
SELECT count() FROM numbers(100) WHERE materialize(materialize('online'::String)) = 'online';

SELECT count() FROM numbers(100) WHERE materialize(CAST(NULL AS Nullable(UInt8)));

SELECT count() FROM numbers(100) WHERE isConstant(materialize('online'));

SELECT count() FROM numbers(10) WHERE materialize(now()) > toDateTime('1970-01-01');

-- surviving non-filter outputs must not be folded - y must still look non-Const downstream
SELECT isConstant(y) FROM (SELECT materialize(1) = 1 AS y FROM numbers(1)) WHERE materialize(1) = 1;

-- like()'s ESCAPE arg must stay non-Const-foldable through materialize - runtime still raises
SELECT like('50%off', '50#%off', materialize('#')); -- { serverError ILLEGAL_COLUMN }

-- planning must not eagerly evaluate the lazy then-branch of `if` -
-- toFloat64('x86_74') would throw at fold time without the short-circuit guard
SELECT count() > 0 FROM (
    EXPLAIN PLAN SELECT count() FROM numbers(1)
    WHERE if(equals(materialize('abc'), 'aws.lambda.duration'),
             toFloat64(materialize('x86_74')) < 50,
             0)
    SETTINGS short_circuit_function_evaluation = 'enable'
);

