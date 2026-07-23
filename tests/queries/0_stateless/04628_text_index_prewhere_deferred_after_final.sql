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
