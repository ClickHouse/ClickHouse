-- Tags: stateless

-- Test for the OrderBy aggregate function combinator.
-- Implements f(x ORDER BY y [DESC] [LIMIT N]) syntax sugar that gets
-- transformed into the parametric form fOrderBy('0 [DESC] [LIMIT N]')(x, y) by the analyzer.

DROP TABLE IF EXISTS test_order_by_combinator;

CREATE TABLE test_order_by_combinator (
    user_id UInt32,
    ts DateTime,
    value String,
    amount Float64
) ENGINE = MergeTree() ORDER BY user_id;

INSERT INTO test_order_by_combinator VALUES
    (1, '2024-01-01 10:00:00', 'b', 10.0),
    (1, '2024-01-01 09:00:00', 'a', 20.0),
    (1, '2024-01-01 11:00:00', 'c', 30.0),
    (2, '2024-01-01 12:00:00', 'd', 5.0),
    (2, '2024-01-01 10:00:00', 'e', 15.0);

-- Basic ORDER BY ASC
SELECT '-- basic ASC';
SELECT user_id, groupArray(value ORDER BY ts) AS arr
FROM test_order_by_combinator GROUP BY user_id ORDER BY user_id;

-- ORDER BY DESC
SELECT '-- DESC';
SELECT user_id, groupArray(value ORDER BY ts DESC) AS arr
FROM test_order_by_combinator GROUP BY user_id ORDER BY user_id;

-- ORDER BY with LIMIT
SELECT '-- LIMIT';
SELECT user_id, groupArray(value ORDER BY ts LIMIT 2) AS arr
FROM test_order_by_combinator GROUP BY user_id ORDER BY user_id;

-- ORDER BY DESC with LIMIT (top-N)
SELECT '-- DESC LIMIT';
SELECT user_id, groupArray(value ORDER BY ts DESC LIMIT 2) AS arr
FROM test_order_by_combinator GROUP BY user_id ORDER BY user_id;

-- Multi-key ORDER BY
SELECT '-- multi-key';
SELECT groupArray(value ORDER BY user_id ASC, ts DESC) FROM test_order_by_combinator;

-- nested function: sum
SELECT '-- sum';
SELECT user_id, sum(amount ORDER BY ts LIMIT 2) FROM test_order_by_combinator GROUP BY user_id ORDER BY user_id;

-- Parametric form (direct combinator invocation)
SELECT '-- parametric';
SELECT user_id, groupArrayOrderBy('0 ASC')(value, ts) FROM test_order_by_combinator GROUP BY user_id ORDER BY user_id;

-- NULLS handling
SELECT '-- NULLS FIRST';
SELECT groupArray(v ORDER BY ts ASC NULLS FIRST) FROM (
    SELECT 'b' AS v, toNullable(2) AS ts UNION ALL
    SELECT 'a' AS v, toNullable(NULL) AS ts UNION ALL
    SELECT 'c' AS v, toNullable(1) AS ts
);

SELECT '-- NULLS LAST';
SELECT groupArray(v ORDER BY ts ASC NULLS LAST) FROM (
    SELECT 'b' AS v, toNullable(2) AS ts UNION ALL
    SELECT 'a' AS v, toNullable(NULL) AS ts UNION ALL
    SELECT 'c' AS v, toNullable(1) AS ts
);

-- Distributed aggregation: -State and finalizeAggregation
SELECT '-- distributed';
SELECT finalizeAggregation(state) FROM (
    SELECT groupArrayOrderByState('0 ASC')(value, ts) AS state
    FROM test_order_by_combinator WHERE user_id = 1
);

-- Empty input
SELECT '-- empty';
SELECT groupArray(toString(number) ORDER BY number) FROM numbers(0);

DROP TABLE test_order_by_combinator;

-- Different LIMIT values are different aggregate functions
SELECT '-- different limits';
SELECT groupArray(number ORDER BY number LIMIT 1), groupArray(number ORDER BY number LIMIT 2) FROM numbers(5);

-- Parameters of the nested function are not mistaken for the LIMIT
SELECT '-- nested parameters';
SELECT uniqUpTo(3)(number ORDER BY number), uniqUpToOrderBy('0 ASC', 3)(number, number) FROM numbers(10);

-- LIMIT in the parametric form
SELECT '-- parametric LIMIT';
SELECT groupArray(v ORDER BY k DESC LIMIT 1), groupArrayOrderBy('0 DESC LIMIT 1')(v, k) FROM values('v String, k Int32', ('a', 1), ('b', 2));

-- Projection names reflect the sort direction
SELECT '-- projection names';
SELECT groupArray(v ORDER BY k ASC), groupArray(v ORDER BY k DESC) FROM values('v String, k Int32', ('a', 1), ('b', 2), ('c', 3)) FORMAT TSVWithNames;

-- Rows with a NULL sort key are not skipped when the nested argument is also Nullable
SELECT '-- nullable argument and key';
SELECT groupArray(v ORDER BY k NULLS FIRST), groupArray(v ORDER BY k)
FROM values('v Nullable(String), k Nullable(Int32)', ('a', 1), (NULL, 2), ('c', NULL), ('d', 0));
SELECT sum(x ORDER BY y), toTypeName(sum(x ORDER BY y)) FROM values('x Nullable(Int32), y Int32', (1, 1), (NULL, 2));

-- Finalizing a state produced by the nested -State function
SELECT '-- nested state';
SELECT finalizeAggregation(st) FROM (SELECT groupArrayState(v ORDER BY k DESC) AS st FROM values('v String, k Int32', ('a', 1), ('b', 2)));

-- DISTINCT together with ORDER BY
SELECT '-- distinct';
SELECT groupArray(DISTINCT number % 3 ORDER BY number % 3 DESC) FROM numbers(10);

-- Many groups and blocks, merged from several threads
SELECT '-- group by';
SELECT k, groupArray(number ORDER BY number DESC LIMIT 2) FROM numbers(100) GROUP BY number % 3 AS k ORDER BY k SETTINGS max_threads = 4, max_block_size = 7;

-- The rewritten function is sent to remote shards
SELECT '-- remote';
SELECT groupArray(number ORDER BY -number LIMIT 3) FROM remote('127.0.0.{1,2}', numbers(5));

-- Invalid usages
SELECT '-- errors';
SELECT groupArrayOrderBy('100 DESC')(number, number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayOrderBy('0 DESC WITH FILL')(number, number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayOrderBy('0 DESC LIMIT 1 xx')(number, number) FROM numbers(3); -- { serverError SYNTAX_ERROR }
SELECT groupArrayOrderBy(1)(number, number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayOrderBy('0 ASC')(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT groupArray(number ORDER BY number WITH FILL) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
SELECT lower(toString(number) ORDER BY number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
