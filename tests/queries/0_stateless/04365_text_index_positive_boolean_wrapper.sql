-- Text index condition looks through positive boolean wrappers around a supported atom,
-- so `hasToken(s, 'x') = true` prunes the same granules as the bare `hasToken(s, 'x')`.
-- See https://github.com/ClickHouse/ClickHouse/issues/110012

SET enable_analyzer = 1;
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

-- One row per granule (index_granularity = 1) so the granule counts asserted below are stable
-- regardless of CI-randomized merge-tree settings. index_granularity_bytes must not be 0 (0 disables
-- adaptive granularity, which warns to stderr when CI randomizes min_rows/min_bytes_for_wide_part and
-- fails Fast test); a large value never caps granules below the index_granularity = 1 row limit.
CREATE TABLE tab (id UInt32, s String, INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 10485760;

INSERT INTO tab SELECT number, if(number = 42, 'the rare token here', concat('common filler ', toString(number))) FROM numbers(128);

-- Assertions key off the text skip index by name (`Name: idx`) so they isolate text-index behavior
-- from unrelated primary-key / constant-folding pruning that the merged-with-master build may add.
-- Positive wrappers: the text index is applied AND prunes to a single granule, like the bare atom.
SELECT 'bare', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare'));
SELECT '= true', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = true);
SELECT '!= false', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') != false);
SELECT 'IN (true)', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IN (true));
SELECT 'IS TRUE', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IS TRUE);
SELECT 'true = atom', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE true = hasToken(s, 'rare'));

-- Works through a wrapped supported atom other than hasToken (equals).
SELECT 'equals = true', countIf(explain ILIKE '%Name: idx%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE (s = 'the rare token here') = true);

-- Negative wrappers give no pruning benefit (NOT of a granule mask is always true), so the text
-- index must NOT be applied.
SELECT '= false no prune', countIf(explain ILIKE '%Name: idx%') = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = false);
SELECT '!= true no prune', countIf(explain ILIKE '%Name: idx%') = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') != true);

-- `= 5` is not a boolean spelling: leave to row-level evaluation, the text index must NOT be applied.
SELECT '= 5 no prune', countIf(explain ILIKE '%Name: idx%') = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = 5);

-- With transform_null_in = 1 the analyzer rewrites `IN` to `nullIn`; the wrapper must still recognize
-- it and prune. A set with a non-truthy member (`IN (true, false)`) must NOT prune even under null_in.
SELECT 'IN (true) null_in', countIf(explain ILIKE '%Name: idx%') = 1 AND countIf(explain ILIKE '%Granules: 1/128%') = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IN (true) SETTINGS transform_null_in = 1);
SELECT 'IN (true,false) null_in no prune', countIf(explain ILIKE '%Name: idx%') = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IN (true, false) SETTINGS transform_null_in = 1);

-- The bare inner atom of a positive wrapper is still optimized for direct read independently: the
-- `__text_index_idx_...` virtual column must appear in the plan for `= true` and `IN (true)`, exactly as
-- for the bare atom. (Uses `count(explain) > 0` on the actions to stay robust to plan-format churn.)
SELECT 'bare direct read', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare')) WHERE explain ILIKE '%__text_index_idx%';
SELECT '= true direct read', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') = true) WHERE explain ILIKE '%__text_index_idx%';
SELECT 'IN (true) direct read', count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(s, 'rare') IN (true)) WHERE explain ILIKE '%__text_index_idx%';

-- Results stay correct in every case.
SELECT 'results',
    countIf(hasToken(s, 'rare')),
    countIf(hasToken(s, 'rare') = true),
    countIf(hasToken(s, 'rare') != false),
    countIf(hasToken(s, 'rare') IN (true)),
    countIf(hasToken(s, 'rare') IS TRUE),
    countIf(hasToken(s, 'rare') = false)
FROM tab;

SELECT 'results null_in', countIf(hasToken(s, 'rare') IN (true)) FROM tab SETTINGS transform_null_in = 1;

DROP TABLE tab;
