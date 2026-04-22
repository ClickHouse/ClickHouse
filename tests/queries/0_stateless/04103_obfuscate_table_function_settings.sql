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

SELECT '--- empty seed produces different output each invocation (not always equal) ---';
-- Two subsequent calls with empty seed produce two independent random seeds, so
-- the outputs coincide only with negligible probability (1 in 2^64) — they are
-- essentially never equal. With eight distinct source values and eight sampled
-- rows, the probability that two independent random permutation-like streams
-- happen to agree is vanishing, so "equal = 0" is a reliable expectation.
WITH
    a AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8)),
    b AS (SELECT groupArray(number) AS xs FROM (SELECT * FROM obfuscate(SELECT * FROM numbers(8)) LIMIT 8))
SELECT (a.xs = b.xs) AS equal FROM a, b;

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
