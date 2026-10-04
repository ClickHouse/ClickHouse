-- Direct read from text index must not rewrite a PREWHERE that is deferred after FINAL,
-- otherwise the post-FINAL filter keeps the old DAG while the read step switches to index columns
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET query_plan_direct_read_from_text_index = 1;
SET apply_prewhere_after_final = 1;

DROP TABLE IF EXISTS t_text_defer_final;

CREATE TABLE t_text_defer_final
(
    k Int32,
    text String,
    v UInt64,
    INDEX idx(text) TYPE text(tokenizer = ngrams(3))
)
ENGINE = ReplacingMergeTree(v)
ORDER BY k;

INSERT INTO t_text_defer_final VALUES (1, 'hello world', 1), (2, 'goodbye world', 1);
INSERT INTO t_text_defer_final VALUES (1, 'nothing here', 2);

SELECT '= deferred prewhere is not rewritten to a direct index read =';
SELECT count() FROM (EXPLAIN actions=1 SELECT k FROM t_text_defer_final FINAL PREWHERE hasPhrase(text, 'hello')) WHERE explain LIKE '%__text_index_idx%';
SELECT count() FROM (EXPLAIN actions=1 SELECT k FROM t_text_defer_final FINAL PREWHERE hasPhrase(text, 'hello')) WHERE explain LIKE '%Deferred prewhere filter column%';

-- whole-token needles: the deferred filter currently runs without the index tokenizer rewrite,
-- so the expected results must not depend on it
SELECT '= the filter sees post-FINAL rows only =';
SELECT k FROM t_text_defer_final FINAL PREWHERE hasPhrase(text, 'hello');
SELECT k FROM t_text_defer_final FINAL PREWHERE hasPhrase(text, 'goodbye');

DROP TABLE t_text_defer_final;

SELECT '= a later text-index rewrite reaches the deferred prewhere =';

DROP TABLE IF EXISTS t_text_defer_final_pp;

CREATE TABLE t_text_defer_final_pp
(
    k Int32,
    text String,
    v UInt64,
    INDEX idx(text) TYPE text(tokenizer = ngrams(3), preprocessor = lower(text))
)
ENGINE = ReplacingMergeTree(v)
ORDER BY k;

-- k = 1 keeps the matching mixed-case row as the FINAL winner (v = 2 beats v = 1);
-- k = 2 never matches, so the deduplication stays meaningful
INSERT INTO t_text_defer_final_pp VALUES (1, 'Nothing Here', 1), (2, 'Goodbye World', 1);
INSERT INTO t_text_defer_final_pp VALUES (1, 'Hello World', 2);

-- The predicate must also appear in WHERE: a deferred PREWHERE is excluded from index analysis and
-- `text` is not a sorting-key column, so the WHERE copy is the only thing that registers the text
-- index and lets a later rewrite fire. Mixed case on disk plus a lower-case needle means only the
-- preprocessor-rewritten predicate matches, so a stale deferred filter would return nothing.
-- `use_skip_indexes_if_final` is randomized by the runner and unregisters the index when false.
SELECT k FROM t_text_defer_final_pp FINAL PREWHERE hasPhrase(text, 'hello world') WHERE hasPhrase(text, 'hello world')
SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1;

SELECT count() FROM (EXPLAIN actions=1 SELECT k FROM t_text_defer_final_pp FINAL PREWHERE hasPhrase(text, 'hello world') WHERE hasPhrase(text, 'hello world') SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1) WHERE explain LIKE '%Deferred prewhere filter column%';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT k FROM t_text_defer_final_pp FINAL PREWHERE hasPhrase(text, 'hello world') WHERE hasPhrase(text, 'hello world') SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1) WHERE explain LIKE '%Name: idx%';

SELECT '= a deferred prewhere does not borrow the preprocessor from a sibling where =';

-- The deferred predicate is excluded from index analysis, so it must run raw: mixed case on disk against a
-- lower-case needle matches nothing, even though the sibling WHERE makes the index useful.
SELECT count() FROM t_text_defer_final_pp FINAL PREWHERE hasPhrase(text, 'hello world') WHERE hasPhrase(text, 'world')
SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1;

-- Control: the same sibling WHERE with a raw-matching deferred prewhere counts both rows, so the zero above
-- comes from the prewhere and not from the where.
SELECT count() FROM t_text_defer_final_pp FINAL PREWHERE hasPhrase(text, 'World') WHERE hasPhrase(text, 'world')
SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1;

-- The deferred filter keeps the tokenizer rewrite but not the preprocessor.
SELECT count() FROM (EXPLAIN actions=1 SELECT k FROM t_text_defer_final_pp FINAL PREWHERE hasPhrase(text, 'hello world') WHERE hasPhrase(text, 'world') SETTINGS use_skip_indexes = 1, use_skip_indexes_if_final = 1) WHERE explain LIKE '%Deferred prewhere filter column%' AND explain LIKE '%lower(%';

DROP TABLE t_text_defer_final_pp;
