-- Regression test: text-index direct-read rewrite should fire for ALIAS
-- columns that derive from a `Map`, not only for MATERIALIZED ones.
--
-- Covers the observable pattern from the bug report — a synthetic
-- `Array(String)` built from a Map and indexed as text — with a constant
-- embedded in the indexed expression (here: the `'sentinel'` literal inside
-- `arrayConcat`). That literal has the analyzer-style `_String` suffix in the
-- filter DAG and the AST-style bare form in the index `sample_block`;
-- without the fix the ALIAS case fails to match and the `__text_index_*`
-- rewrite does not fire.

SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    attrs Map(String, String),
    tokens_m Array(String) MATERIALIZED arrayConcat(mapKeys(attrs), ['sentinel']),
    tokens_a Array(String) ALIAS        arrayConcat(mapKeys(attrs), ['sentinel']),
    INDEX idx_tokens_m tokens_m TYPE text(tokenizer = 'array') GRANULARITY 100000000,
    INDEX idx_tokens_a tokens_a TYPE text(tokenizer = 'array') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, {'host': '192.168.1.1', 'service': 'web'}),
    (2, {'host': '5.6.7.8',     'service': 'api'}),
    (3, {'namespace': 'prod'});

-- Both columns expand to the same expression, so `has` must return the same count.
SELECT 'materialized', count() FROM tab WHERE has(tokens_m, 'host');
SELECT 'alias',        count() FROM tab WHERE has(tokens_a, 'host');

-- Both plans must reference the `__text_index_*` virtual column, proving the
-- direct-read rewrite fired.
SELECT 'materialized EXPLAIN rewritten', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE has(tokens_m, 'host')
)
WHERE explain LIKE '%__text_index_idx_tokens_m_has%';

SELECT 'alias EXPLAIN rewritten', count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE has(tokens_a, 'host')
)
WHERE explain LIKE '%__text_index_idx_tokens_a_has%';

DROP TABLE tab;
