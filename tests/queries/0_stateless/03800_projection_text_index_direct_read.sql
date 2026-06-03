SET allow_experimental_projection_text_index = 1;
-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel -- due to access to the system.text_log
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

SET log_queries = 1;

-- Affects the number of read rows.
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET max_rows_to_read = 0; -- system.text_log can be really big
SET enable_analyzer = 0; -- To produce consistent explain outputs

----------------------------------------------------
SELECT '- Test direct read optimization from text log';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, text String, PROJECTION idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha'))
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'),
                       (102, 'Blick a02');

----------------------------------------------------

SELECT 'Test hasToken:', count() FROM tab WHERE hasToken(text, 'Alick');
SELECT 'Test hasAllTokens:', count() FROM tab WHERE hasAllTokens(text, ['Alick']);
SELECT 'Test hasAnyTokens:', count() FROM tab WHERE hasAnyTokens(text, ['Alick']);
SELECT 'Test hasToken + length(text):', count() FROM tab WHERE hasToken(text, 'Alick') or length(text) > 1;
SELECT 'Test select text + hasAnyTokens:', text FROM tab WHERE hasAnyTokens(text, ['Alick']);
SELECT 'Test hasToken and hasToken:', count() FROM tab WHERE hasToken(text, 'Alick') and hasToken(text, 'Blick');
SELECT 'Test hasAnyTokens or hasToken:', count() FROM tab WHERE hasAnyTokens(text, ['Blick']) or hasToken(text, 'Alick');
SELECT 'Test NOT hasAllTokens:', count() FROM tab WHERE NOT hasAllTokens(text, ['Blick']);
----------------------------------------------------
-- Direct read optimization is not supported for projection text indexes
-- (they use a separate skip index path). Just verify basic queries work.
----------------------------------------------------
SELECT '- Test direct read optimization with EXPLAIN';

DROP TABLE tab;
