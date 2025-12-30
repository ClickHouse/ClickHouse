-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel -- due to access to the system.text_log
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

SET log_queries = 1;

-- Affects the number of read rows.
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET max_rows_to_read = 0; -- system.text_log can be really big
SET enable_analyzer = 0; -- To produce consistent explain outputs

----------------------------------------------------
SELECT '- Test direct read optimization from text log';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, text String, INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
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
-- Now check the logs all at once (one by one is too slow)
----------------------------------------------------
SYSTEM FLUSH LOGS text_log;

SELECT message
FROM (
     SELECT event_time_microseconds, message FROM system.text_log
     WHERE logger_name = 'optimizeDirectReadFromTextIndex' AND startsWith(message, 'Added:')
     ORDER BY event_time_microseconds DESC LIMIT 8
)
ORDER BY event_time_microseconds ASC;

----------------------------------------------------
-- Now check that EXPLAIN produces the expected output for the same queries.
-- So this AFTER checking the text_log otherwise the entries will be duplicated.
----------------------------------------------------
SELECT '- Test direct read optimization with EXPLAIN';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test hasToken:', count() FROM tab WHERE hasToken(text, 'Alick') SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test hasAllTokens:', count() FROM tab WHERE hasAllTokens(text, ['Alick']) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test hasAnyTokens:', count() FROM tab WHERE hasAnyTokens(text, ['Alick']) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test hasToken + length(text):', count() FROM tab WHERE hasToken(text, 'Alick') or length(text) > 1 SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test select text + hasAnyTokens:', text FROM tab WHERE hasAnyTokens(text, ['Alick']) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Prewhere filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test hasToken and hasToken:', count() FROM tab WHERE hasToken(text, 'Alick') and hasToken(text, 'Blick') SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Prewhere filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test hasAnyTokens or hasToken:', count() FROM tab WHERE hasAnyTokens(text, ['Blick']) or hasToken(text, 'Alick') SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT 'Test NOT hasAllTokens:', count() FROM tab WHERE NOT hasAllTokens(text, ['Blick']) SETTINGS use_skip_indexes_on_data_read = 1
) WHERE explain LIKE '%Filter column:%';

DROP TABLE tab;
