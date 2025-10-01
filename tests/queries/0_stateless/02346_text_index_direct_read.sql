-- Tags: no-fasttest
-- no-fasttest: It can be slow

SET log_queries = 1;

-- Affects the number of read rows.
SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET max_rows_to_read = 0; -- system.text_log can be really big

----------------------------------------------------
SELECT 'Test direct read optimization from text log';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab(k UInt64, text String, INDEX idx(text) TYPE text(tokenizer = 'default') GRANULARITY 1)
            ENGINE = MergeTree() ORDER BY k
            SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

INSERT INTO tab VALUES (101, 'Alick a01'),
                       (102, 'Blick a02');


----------------------------------------------------
SELECT 'Test hasToken';

SELECT count() FROM tab WHERE hasToken(text, 'Alick');

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time DESC LIMIT 1;

----------------------------------------------------
SELECT 'Test searchAll';

SELECT count() FROM tab WHERE searchAll(text, ['Alick']);

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;

----------------------------------------------------
SELECT 'Test searchAny';

SELECT count() FROM tab WHERE searchAny(text, ['Alick']);

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;

----------------------------------------------------
-- Adds column, but keeps the original
SELECT 'Test hasToken + length(text)';

SELECT count() FROM tab WHERE hasToken(text, 'Alick') or length(text) > 1;

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;

----------------------------------------------------
-- Adds column, but keeps the original
SELECT 'Test select text + hasToken';

SELECT text FROM tab WHERE searchAny(text, ['Alick']);

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;


----------------------------------------------------
-- 2 substitutions
----------------------------------------------------
SELECT 'Test hasToken and hasToken';

SELECT count() FROM tab WHERE hasToken(text, 'Alick') and hasToken(text, 'Blick');

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;

----------------------------------------------------
SELECT 'Test searchAny or hasToken';

SELECT count() FROM tab WHERE searchAny(text, ['Blick']) or hasToken(text, 'Alick');

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;


----------------------------------------------------
SELECT 'Test NOT searchAll';

SELECT count() FROM tab WHERE NOT searchAll(text, ['Blick']);

SYSTEM FLUSH LOGS text_log;

SELECT message FROM system.text_log WHERE
       (logger_name = 'optimizeDirectReadFromTextIndex') and
       startsWith(message, 'Added:') ORDER BY event_time_microseconds DESC LIMIT 1;


DROP TABLE IF EXISTS tab;
