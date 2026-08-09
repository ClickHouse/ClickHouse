-- Issue #78166: WHERE over UNION where each branch wraps a const in materialize

SET enable_analyzer = 1;

SELECT uniq(id) FROM (
    SELECT 'online' AS event_type, 'i' AS id FROM numbers(100)
    UNION ALL
    SELECT 'click' AS event_type, '2' AS id FROM numbers(100)
) AS t
WHERE event_type = 'online';

-- folded filter condition becomes a plain constant in the plan
SELECT 'folded filters', countIf(explain LIKE '%Filter column: 1%' OR explain LIKE '%Filter column: 0%')
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
SELECT 'both folded', countIf(explain LIKE '%Filter column: 0%')
FROM (
    EXPLAIN PLAN actions = 1
    SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 3
);

-- one branch passes, one rejects
SELECT x FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) WHERE x > 1 ORDER BY x;

-- standalone WHERE materialize(const) = const
SELECT count() FROM numbers(100) WHERE materialize('online'::String) = 'online';
SELECT 'simple folded', countIf(explain LIKE '%Filter column: 1%')
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

-- predicate function not in the value-only whitelist - filter stays as is, runtime still raises
SELECT count() FROM numbers(1) WHERE like('50%off', '50#%off', materialize('#')); -- { serverError ILLEGAL_COLUMN }

-- buried materialize under a non-whitelisted child of a non-whitelisted parent - still no fold
SELECT count() FROM numbers(1) WHERE like('50%off', '50#%off', concat(materialize('#'), '')); -- { serverError ILLEGAL_COLUMN }

-- mixed String/non-String comparison raises at analysis (header build on non-const String),
-- before any fold runs - the exception must be preserved
SELECT count() FROM numbers(1) WHERE materialize('1') = toUInt8(1); -- { serverError NO_COMMON_TYPE }
SELECT count() FROM numbers(1) WHERE materialize('257') != toUInt8(1); -- { serverError NO_COMMON_TYPE }

-- folding and's second arg is safe: an unconvertible const comparison yields false, not an exception
SELECT count() FROM numbers(1) WHERE and(0, materialize(tuple(1)) = '(1') SETTINGS short_circuit_function_evaluation = 'enable';

-- empty rowset, toFloat64 is not whitelisted so no speculative fold, runtime skips the WHERE
SELECT count() FROM numbers(0) WHERE toFloat64(materialize('x86_74')) < 50;

-- `if` not in the whitelist - planning doesn't evaluate the lazy then-branch
SELECT count() > 0 FROM (
    EXPLAIN PLAN SELECT count() FROM numbers(1)
    WHERE if(equals(materialize('abc'), 'aws.lambda.duration'),
             toFloat64(materialize('x86_74')) < 50,
             0)
    SETTINGS short_circuit_function_evaluation = 'enable'
);


-- comparison is only invariant to constness when both sides are strings or neither is:
-- `executeWithConstString` casts a *constant* String/FixedString operand to the other operand's
-- type and is unreachable for a materialized one, so folding a mixed comparison through
-- `materialize` would switch the dispatch. Such a filter must stay in the plan
SELECT 'mixed string comparison not folded', countIf(explain LIKE '%Filter column: materialize%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1) WHERE materialize(toUInt8(1)) = '1');
SELECT count() FROM numbers(1) WHERE materialize(toUInt8(1)) = '1';
SELECT count() FROM numbers(1) WHERE materialize(toUInt8(1)) = '257';

-- same-typed comparisons stay foldable
SELECT count() FROM numbers(100) WHERE materialize('online'::String) = materialize('online'::String);
SELECT count() FROM numbers(100) WHERE materialize(1) = materialize(2);

-- `and` / `or` fold only when every argument folds to a constant: whether runtime evaluates the
-- arguments after a decisive one depends on `short_circuit_function_evaluation` (the `disable` mode
-- promises eager evaluation, including exceptions), which the fold cannot see. A decisive constant
-- with a non-foldable sibling therefore stays in the plan (a literal `0` would be folded earlier,
-- by the analyzer, so the decisive argument is wrapped in `materialize`)
SELECT 'decisive and with non-const sibling not folded', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(100) WHERE and(materialize(0), number = 1));
SELECT count() FROM numbers(100) WHERE and(materialize(0), number = 1);
SELECT count() FROM numbers(100) WHERE or(materialize(1), number = 1);

-- all-constant `and` / `or` still fold, through `materialize` wrappers
SELECT 'all-const and folded', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(100) WHERE and(materialize(0), materialize(1)));
SELECT count() FROM numbers(100) WHERE and(materialize(0), materialize(1));
SELECT count() FROM numbers(100) WHERE or(materialize(1), materialize(0));

-- an unreachable throwing argument must not raise under short-circuit evaluation, and must still
-- raise under `disable`, which evaluates it eagerly. `system.one` (no range analysis of the
-- predicate on the read) and `query_plan_merge_filters = 0` (no and-chain splitting into separate
-- FilterTransforms) keep the conjunction evaluated as a single expression
SELECT count() FROM system.one WHERE and(materialize(0), throwIf(dummy >= 0))
    SETTINGS short_circuit_function_evaluation = 'enable', query_plan_merge_filters = 0;
SELECT count() FROM system.one WHERE and(materialize(0), throwIf(dummy >= 0))
    SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- a NULL argument is not decisive for `and`, and the result stays Nullable
SELECT count() FROM numbers(100) WHERE and(materialize(1), CAST(NULL AS Nullable(UInt8)));
SELECT count() FROM numbers(100) WHERE and(materialize(0), CAST(NULL AS Nullable(UInt8)));
SELECT count() FROM numbers(100) WHERE or(CAST(NULL AS Nullable(UInt8)), materialize(1));

-- a filter that only meets the `materialize` after `tryPushDownFilter` cloned it into the branches
-- still folds - the fold runs on `FilterStep` construction, not only from `tryMergeExpressions`
SELECT 'pushdown through sorting', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1
    SELECT * FROM (SELECT number, materialize(1) AS m FROM numbers(10) ORDER BY number) WHERE m = 5);
SELECT 'pushdown through aggregation', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1
    SELECT * FROM (SELECT number, materialize(1) AS m FROM numbers(10) GROUP BY number, m) WHERE m = 5);
SELECT 'pushdown into union all branches', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1
    SELECT * FROM (
        SELECT number, materialize(1) AS m FROM numbers(10)
        UNION ALL SELECT number, materialize(2) AS m FROM numbers(10)
        UNION ALL SELECT number, materialize(3) AS m FROM numbers(10)
    ) WHERE m = 5);
SELECT 'pushdown into union distinct branches', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1
    SELECT * FROM (
        SELECT number, materialize(1) AS m FROM numbers(10)
        UNION DISTINCT SELECT number, materialize(2) AS m FROM numbers(10)
    ) WHERE m = 5);
SELECT count() FROM (
    SELECT number, materialize(1) AS m FROM numbers(10)
    UNION ALL SELECT number, materialize(2) AS m FROM numbers(10)
) WHERE m = 5;

-- a filter pushed down over a JOIN folds inside the fresh FilterStep; the fold prunes the original
-- predicate node from the moved-in DAG, so the caller must not keep references into it (the
-- "Pushed down filter ... side of join" log line read a freed name - caught by ASan)
SELECT 'pushdown over join', countIf(explain LIKE '%Filter column: 0%')
FROM (EXPLAIN PLAN actions = 1
    SELECT t1.a FROM (SELECT number AS a, materialize(1) AS m FROM numbers(3)) t1
    JOIN (SELECT number AS a FROM numbers(3)) t2 ON t1.a = t2.a
    WHERE m = 5);
SELECT count() FROM (SELECT number AS a, materialize(1) AS m FROM numbers(3)) t1
JOIN (SELECT number AS a FROM numbers(3)) t2 ON t1.a = t2.a
WHERE m = 5;

-- tuple comparison decomposes a constant tuple into constant element columns, so a nested
-- string / non-string element pair reaches `executeWithConstString` only on the folded path -
-- the invariance check recurses through tuple elements and such filters must stay in the plan
SELECT 'mixed tuple comparison not folded', countIf(explain LIKE '%Filter column: materialize%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1) WHERE materialize(tuple(toUInt8(1))) = tuple('1'));
SELECT count() FROM numbers(1) WHERE materialize(tuple(toUInt8(1))) = tuple('1');
SELECT 'nested mixed tuple comparison not folded', countIf(explain LIKE '%Filter column: materialize%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1) WHERE materialize(tuple(tuple(toUInt8(1)), 1)) = tuple(tuple('1'), 1));
-- with the string on the materialized side the nested comparison raises - the fold must not
-- swallow the exception
SELECT count() FROM numbers(1) WHERE materialize(tuple('1')) = tuple(toUInt8(1)); -- { serverError NO_COMMON_TYPE }
-- same-shaped tuples still fold
SELECT 'same-typed tuple folded', countIf(explain LIKE '%Filter column: 1%')
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM numbers(1) WHERE materialize(tuple('1', 2)) = tuple('1', 2));
SELECT count() FROM numbers(1) WHERE materialize(tuple('1', 2)) = tuple('1', 2);
