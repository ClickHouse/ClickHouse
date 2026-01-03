-- This test checks if we choose the efficient *Window aggregate function implementation
-- when used in a window function context.

-- Difficult to test with old analyzer because it does not show resolved function names in EXPLAIN PLAN
SET enable_analyzer = 1;

SELECT 'Simple non-window function context';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersV(number, number % 3)
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersVDistinctIf(number, number % 3, 1)
    FROM numbers(5)
) WHERE line LIKE '%Function%';


SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersVBiasCorrected(number, number % 3)
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersVBiasCorrectedDistinctIf(number, number % 3, 1)
    FROM numbers(5)
) WHERE line LIKE '%Function%';


SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        theilsU(number, number % 3)
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        theilsUDistinctIf(number, number % 3, 1)
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        contingency(number, number % 3)
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        contingencyDistinctIf(number, number % 3, 1)
    FROM numbers(5)
) WHERE line LIKE '%Function%';


SELECT 'Simple window function context';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersV(number, number % 3) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersVDistinctIf(number, number % 3, 1) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';


SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersVBiasCorrected(number, number % 3) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        cramersVBiasCorrectedDistinctIf(number, number % 3, 1) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';


SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        theilsU(number, number % 3) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        theilsUDistinctIf(number, number % 3, 1) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        contingency(number, number % 3) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';

SELECT ltrim(*) as line
FROM (
    EXPLAIN PLAN actions = 1
    SELECT
        contingencyDistinctIf(number, number % 3, 1) OVER()
    FROM numbers(5)
) WHERE line LIKE '%Function%';
