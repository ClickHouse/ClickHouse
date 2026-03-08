-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Verify that direct read optimization for text index is applied
-- even when `use_skip_indexes_on_data_read` is disabled.

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 0;
SET enable_analyzer = 1; -- To produce consistent explain outputs

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, text String, INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'), (102, 'Blick a02');

-- Check that queries return correct results
SELECT 'Test hasToken:', count() FROM tab WHERE hasToken(text, 'Alick');
SELECT 'Test hasAllTokens:', count() FROM tab WHERE hasAllTokens(text, ['Alick']);
SELECT 'Test hasAnyTokens:', count() FROM tab WHERE hasAnyTokens(text, ['Alick']);

-- Check that EXPLAIN shows virtual columns from text index direct read optimization
SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasToken(text, 'Alick')
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAllTokens(text, ['Alick'])
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab WHERE hasAnyTokens(text, ['Alick'])
) WHERE explain LIKE '%Filter column:%';

DROP TABLE tab;
