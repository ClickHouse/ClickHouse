-- Tags: stateless

-- Test for the OrderBy aggregate function combinator.
-- Implements f(x ORDER BY y [DESC] [LIMIT N]) syntax sugar that gets
-- transformed into the parametric form fOrderBy('y', x, y) by the analyzer.

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
