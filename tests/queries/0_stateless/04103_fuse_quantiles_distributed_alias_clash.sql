-- Regression: `MULTIPLE_EXPRESSIONS_FOR_ALIAS` on distributed queries that reference the same
-- `quantile(p)(x)` in SELECT, HAVING, and ORDER BY when `optimize_syntax_fuse_functions = 1`.
--
-- Before the fix, `FuseFunctionsPass` packed every reference to `quantile(0.99)(x)` into a
-- `quantiles(0.99, 0.99, 0.99)` call and assigned a distinct `arrayElement(..., i)` index to
-- each occurrence, then forced the same `quantile(0.99)(x)` alias onto all of them. When the
-- query tree was serialized back to SQL for distributed dispatch, the three `arrayElement`
-- nodes differed only in their index but shared the alias, and the analyzer tripped
-- `MULTIPLE_EXPRESSIONS_FOR_ALIAS`.
--
-- The pass now deduplicates by quantile level, so identical references share the same
-- `arrayElement` extraction.
-- https://github.com/ClickHouse/ClickHouse/issues/102976

SET optimize_syntax_fuse_functions = 1;

-- Same quantile referenced in SELECT, HAVING, and ORDER BY over a distributed table.
-- Should succeed, not throw `MULTIPLE_EXPRESSIONS_FOR_ALIAS`.
SELECT
    number % 10 AS bucket,
    count() AS c,
    quantile(0.99)(number) AS p99
FROM remote('127.0.0.1', system.numbers)
WHERE number < 1000
GROUP BY bucket
HAVING p99 > 100
ORDER BY p99 DESC
LIMIT 3;

-- Repeating the aggregate expression (not the alias) in HAVING and ORDER BY used to hit the
-- same codepath because alias expansion runs before the fusion pass.
SELECT
    number % 10 AS bucket,
    quantile(0.99)(number) AS p99
FROM remote('127.0.0.1', system.numbers)
WHERE number < 1000
GROUP BY bucket
HAVING quantile(0.99)(number) > 100
ORDER BY quantile(0.99)(number) DESC
LIMIT 3;

-- Mixed levels still fuse into a single `quantiles` call, with each distinct level appearing
-- exactly once regardless of how many times it is referenced.
SELECT
    number % 10 AS bucket,
    quantile(0.5)(number) AS p50,
    quantile(0.99)(number) AS p99
FROM remote('127.0.0.1', system.numbers)
WHERE number < 1000
GROUP BY bucket
HAVING quantile(0.99)(number) > 100 AND quantile(0.5)(number) > 10
ORDER BY quantile(0.99)(number) DESC, quantile(0.5)(number) ASC
LIMIT 3;
