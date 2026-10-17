-- Regression test: text-index direct-read rewrite should fire for ALIAS
-- columns, not only for MATERIALIZED ones.
--
-- The filter DAG uses analyzer-style constant names (`'='_String`) while the
-- index `sample_block` is built by the old `ExpressionAnalyzer`
-- (AST-style `'='`). `MergeTreeIndexConditionText` was originally constructed
-- from a predicate cloned through `ActionsDAGWithInversionPushDown` (which
-- canonicalizes constant names), so the header match worked. But the later
-- optimization pass used the raw filter-DAG node, so `header.has(...)` missed
-- whenever the indexed expression itself had a constant child — which is
-- exactly the `ALIAS concat(s, '=', t)` case.

SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    s String,
    t String,
    stm String MATERIALIZED concat(s, '=', t),
    sta String ALIAS concat(s, '=', t),
    INDEX idx_stm stm TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_sta sta TYPE text(tokenizer = 'array') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab (s, t) VALUES ('a', 'b'), ('a', 'c'), ('c', 'd'), ('e', 'f');

-- Results must be identical for the two equivalent columns.
SELECT 'materialized', count() FROM tab WHERE hasAllTokens(stm, 'a=c');
SELECT 'alias',        count() FROM tab WHERE hasAllTokens(sta, 'a=c');

-- Both plans must reference the `__text_index_*` virtual column, proving the
-- direct-read rewrite fired.
SELECT 'materialized EXPLAIN rewritten', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAllTokens(stm, 'a=c')
)
WHERE explain LIKE '%__text_index_idx_stm_hasAllTokens%';

SELECT 'alias EXPLAIN rewritten', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAllTokens(sta, 'a=c')
)
WHERE explain LIKE '%__text_index_idx_sta_hasAllTokens%';

DROP TABLE tab;
