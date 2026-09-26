-- Regression test for the direct-read half of https://github.com/ClickHouse/ClickHouse/pull/116381.
-- A text index without phrase positions gives `hasPhrase` the `Hint` direct-read mode, and only `Hint`
-- keeps the original predicate next to the index virtual column, so the index tokenizer reaches the
-- row-level function only as an injected third argument. The predicate that executes must be that
-- injected form. `hasAnyTokens` and `hasAllTokens` take the same injected argument, but over a plain
-- text index they get `Exact` mode, which drops the original predicate, so this seam is
-- `hasPhrase`-only here.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;              -- randomized; the index must be eligible for direct read
SET query_plan_direct_read_from_text_index = 1;     -- randomized; the rewrite under test
SET query_plan_text_index_add_hint = 1;             -- randomized; at 0 `hasPhrase` gets `None` mode, not `Hint`
SET optimize_move_to_prewhere = 0;                  -- randomized; keep each predicate in the clause it is written in
SET query_plan_optimize_prewhere = 1;               -- randomized; an explicitly written PREWHERE must stay PREWHERE

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    text String,
    INDEX idx text TYPE text(tokenizer = ngrams(3), support_phrase_search = 0)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) FROM numbers(1000);

-- 'ello50 wor' is a phrase of 'hello50 world50' under `ngrams(3)` but not under `splitByNonAlpha`,
-- so every arm below must report the same 10 rows.
SELECT 'p', count() FROM tab WHERE hasPhrase(text, 'ello50 wor');
SELECT 'not not p', count() FROM tab WHERE NOT (NOT hasPhrase(text, 'ello50 wor'));
SELECT 'p and 1', count() FROM tab WHERE hasPhrase(text, 'ello50 wor') AND 1;
SELECT 'p or 0', count() FROM tab WHERE hasPhrase(text, 'ello50 wor') OR 0;
SELECT 'prewhere not not p', count() FROM tab PREWHERE NOT (NOT hasPhrase(text, 'ello50 wor'));

-- Ground truth: the tokenizer spelled out and the index off, so the rewrite cannot perturb it.
SELECT 'tokenizer spelled out', count() FROM tab WHERE hasPhrase(text, 'ello50 wor', 'ngrams(3)')
    SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

-- The arms above are only meaningful while the index is really read, so assert the rewrite fired.
SELECT 'index read', countIf(position(explain, '__text_index_') > 0) > 0
    FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE NOT (NOT hasPhrase(text, 'ello50 wor')));

DROP TABLE tab;
