-- Text index condition looks through positive boolean wrappers around a supported atom,
-- so `hasToken(s, 'x') = true` prunes the same granules as the bare `hasToken(s, 'x')`.
-- See https://github.com/ClickHouse/ClickHouse/issues/110012

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

-- One row per granule (index_granularity_bytes = 0 disables adaptive granularity) so the granule
-- counts asserted below are stable regardless of CI-randomized merge-tree settings.
CREATE TABLE tab (id UInt32, s String, INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab SELECT number, if(number = 42, 'the rare token here', concat('common filler ', toString(number))) FROM numbers(128);

-- Bare atom prunes to a single granule: this is the baseline every positive wrapper must match.
SELECT 'bare', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare')) WHERE explain ILIKE '%Granules: 1/128%';

-- Positive wrappers must build the same index condition and prune identically.
SELECT '= true', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = true) WHERE explain ILIKE '%Granules: 1/128%';
SELECT '!= false', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') != false) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'IN (true)', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IN (true)) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'IS TRUE', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IS TRUE) WHERE explain ILIKE '%Granules: 1/128%';
SELECT 'true = atom', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE true = hasToken(s, 'rare')) WHERE explain ILIKE '%Granules: 1/128%';

-- Works through a wrapped supported atom other than hasToken (equals).
SELECT 'equals = true', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE (s = 'the rare token here') = true) WHERE explain ILIKE '%Granules%' AND explain NOT ILIKE '%Granules: 128/128%';

-- Negative wrappers give no pruning benefit (NOT of a granule mask is always true), so the index
-- must NOT prune: the full 128/128 granules are read (the index may still be listed).
SELECT '= false no prune', countIf(explain ILIKE '%Granules: 128/128%') = countIf(explain ILIKE '%Granules:%') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = false);
SELECT '!= true no prune', countIf(explain ILIKE '%Granules: 128/128%') = countIf(explain ILIKE '%Granules:%') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') != true);

-- `= 5` is not a boolean spelling: leave to row-level evaluation, no pruning.
SELECT '= 5 no prune', countIf(explain ILIKE '%Granules: 128/128%') = countIf(explain ILIKE '%Granules:%') FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = 5);

-- Results stay correct in every case.
SELECT 'results',
    countIf(hasToken(s, 'rare')),
    countIf(hasToken(s, 'rare') = true),
    countIf(hasToken(s, 'rare') != false),
    countIf(hasToken(s, 'rare') IN (true)),
    countIf(hasToken(s, 'rare') IS TRUE),
    countIf(hasToken(s, 'rare') = false)
FROM tab;

DROP TABLE tab;
