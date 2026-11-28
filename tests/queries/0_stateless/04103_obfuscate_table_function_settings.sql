-- Verify that obfuscate_* settings change the output of the obfuscate table function.
-- LIMIT must be pushed inside the subquery because obfuscate(...) is an infinite source.

SELECT '--- two seeds produce different outputs ---';
WITH
    a AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_seed = 'seed-one')),
    b AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_seed = 'seed-two'))
SELECT (a.xs = b.xs) AS equal FROM a, b;

SELECT '--- same seed produces identical output across invocations ---';
WITH
    a AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_seed = 'stable')),
    b AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8 SETTINGS obfuscate_seed = 'stable'))
SELECT (a.xs = b.xs) AS equal FROM a, b;

SELECT '--- empty seed is accepted and produces a bounded result ---';
-- An empty seed generates a fresh random seed for each query. We only assert that the
-- query succeeds and returns the requested number of rows; asserting that two random
-- runs differ would be probabilistic (for such a tiny domain the outputs can coincide
-- by chance) and could flake.
SELECT count() FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8);

SELECT '--- invalid markov settings are rejected ---';
SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1 SETTINGS obfuscate_markov_order = 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1 SETTINGS obfuscate_markov_frequency_desaturate = 2; -- { serverError BAD_ARGUMENTS }
SELECT * FROM obfuscate(SELECT toString(number) AS s FROM numbers(8)) LIMIT 1 SETTINGS obfuscate_markov_frequency_desaturate = -1; -- { serverError BAD_ARGUMENTS }

SELECT '--- all markov parameters are accepted and do not error ---';
SELECT count()
FROM (
    SELECT s
    FROM obfuscate(SELECT concat('hello world ', toString(number)) AS s FROM numbers(50))
    LIMIT 20
    SETTINGS
        obfuscate_seed = 'markov-test',
        obfuscate_markov_order = 3,
        obfuscate_markov_frequency_cutoff = 1,
        obfuscate_markov_num_buckets_cutoff = 0,
        obfuscate_markov_frequency_add = 1,
        obfuscate_markov_frequency_desaturate = 0.5,
        obfuscate_markov_determinator_sliding_window_size = 4
);
