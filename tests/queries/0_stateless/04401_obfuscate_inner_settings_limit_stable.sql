-- The `obfuscate` table function re-executes its inner query for training and for every
-- generation pass, reusing the same saved AST. The analyzer bakes an inner `SETTINGS limit`
-- (and `offset`) into the query tree as an actual limit operator; `buildQueryTree` clones the
-- input AST before doing so, so the saved inner query is never mutated and the limit is applied
-- identically on every pass. Guard against a regression where the limit would be stripped after
-- the first pass and later passes would re-read the full, unlimited inner query.
--
-- Pinned to the analyzer because there `SETTINGS limit` becomes a pipeline operator. On the old
-- interpreter `SETTINGS limit`/`offset` are result-level limits, which the inner context clears
-- by design (so the outer result limit cannot truncate the training data), so they do not apply
-- to the inner query at all.

SET allow_experimental_analyzer = 1;

-- With the inner limit honored on every pass, `numbers(100) SETTINGS limit = 2` is equivalent to
-- `numbers(2)`: identical training data, identical 2-row passes, identical seed advancement.
-- The two obfuscated streams must therefore match across many passes (here 20 rows = 10 passes).
SELECT '--- inner SETTINGS limit is re-applied on every generation pass ---';
WITH
    a AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(100) SETTINGS limit = 2) LIMIT 20 SETTINGS obfuscate_seed = 'stable')),
    b AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(2)) LIMIT 20 SETTINGS obfuscate_seed = 'stable'))
SELECT (a.xs = b.xs) AS equal FROM a, b;

-- The same query is deterministic across invocations for a fixed seed.
SELECT '--- deterministic across invocations for a fixed seed ---';
WITH
    a AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(100) SETTINGS limit = 2) LIMIT 20 SETTINGS obfuscate_seed = 'stable')),
    b AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT number FROM numbers(100) SETTINGS limit = 2) LIMIT 20 SETTINGS obfuscate_seed = 'stable'))
SELECT (a.xs = b.xs) AS equal FROM a, b;
