-- Tags: no-parallel-replicas

SELECT 'Cursor candidates match a scan';

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET text_index_hint_max_selectivity = 1.;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 0;

DROP TABLE IF EXISTS tab_ref;
DROP TABLE IF EXISTS tab_idx;
DROP TABLE IF EXISTS tab_none;
DROP TABLE IF EXISTS tab_win;

CREATE TABLE tab_ref (id UInt32, message String) ENGINE = MergeTree ORDER BY id;

SELECT 'Cursors need the block index of a posting list codec';
CREATE TABLE tab_idx
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, posting_list_block_size = 256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'pfor';

SELECT 'Without a codec the phrase is evaluated on the column';
CREATE TABLE tab_none
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, posting_list_block_size = 256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'none';

SELECT 'Small marks split the phrase search into windows';
CREATE TABLE tab_win
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1, posting_list_block_size = 256)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'pfor', index_granularity = 64;

SELECT 'hello in 16 segments, needle in 2, unicorn embedded';
INSERT INTO tab_ref
SELECT number, concat(
    if(number % 3 = 0, 'hello world', 'hello clickhouse world'),
    multiIf(number % 16 = 5, ' needle clickhouse rare', number % 16 = 13, ' needle rare', ''),
    if(number IN (1000, 2000, 3000), ' unicorn clickhouse', ''))
FROM numbers(4096);

INSERT INTO tab_idx SELECT id, message FROM tab_ref;
OPTIMIZE TABLE tab_idx FINAL;
INSERT INTO tab_none SELECT id, message FROM tab_ref;
OPTIMIZE TABLE tab_none FINAL;
INSERT INTO tab_win SELECT id, message FROM tab_ref;
OPTIMIZE TABLE tab_win FINAL;

SELECT 'needle clickhouse';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'needle clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'needle clickhouse') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'needle clickhouse') SETTINGS log_comment = '02346_cursors_none';

SELECT 'clickhouse rare';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'clickhouse rare');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse rare') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'clickhouse rare') SETTINGS log_comment = '02346_cursors_none';

SELECT 'needle clickhouse rare';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'needle clickhouse rare');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'needle clickhouse rare') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'needle clickhouse rare') SETTINGS log_comment = '02346_cursors_none';

SELECT 'world needle';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'world needle');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'world needle') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'world needle') SETTINGS log_comment = '02346_cursors_none';

SELECT 'clickhouse needle';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'clickhouse needle');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse needle') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'clickhouse needle') SETTINGS log_comment = '02346_cursors_none';

SELECT 'unicorn clickhouse';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'unicorn clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'unicorn clickhouse') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'unicorn clickhouse') SETTINGS log_comment = '02346_cursors_none';

SELECT 'hello clickhouse';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'hello clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'hello clickhouse') SETTINGS log_comment = '02346_cursors_none';

SELECT 'clickhouse world';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'clickhouse world');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse world') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'clickhouse world') SETTINGS log_comment = '02346_cursors_none';

SELECT 'world hello';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'world hello');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'world hello') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'world hello') SETTINGS log_comment = '02346_cursors_none';

SELECT 'hello clickhouse world';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'hello clickhouse world');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse world') SETTINGS log_comment = '02346_cursors';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_none WHERE hasPhrase(message, 'hello clickhouse world') SETTINGS log_comment = '02346_cursors_none';

SELECT 'Windows skipping marks';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE (id BETWEEN 1000 AND 1100 OR id BETWEEN 2990 AND 3500) AND hasPhrase(message, 'needle clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_win WHERE (id BETWEEN 1000 AND 1100 OR id BETWEEN 2990 AND 3500) AND hasPhrase(message, 'needle clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE (id BETWEEN 1000 AND 1100 OR id BETWEEN 2990 AND 3500) AND hasPhrase(message, 'unicorn clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_win WHERE (id BETWEEN 1000 AND 1100 OR id BETWEEN 2990 AND 3500) AND hasPhrase(message, 'unicorn clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE id > 2000 AND hasPhrase(message, 'needle');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_win WHERE id > 2000 AND hasPhrase(message, 'needle');

SELECT 'Windows in reverse order';
SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE hasPhrase(message, 'needle clickhouse rare') ORDER BY id DESC LIMIT 5);
SELECT groupArray(id) FROM (SELECT id FROM tab_win WHERE hasPhrase(message, 'needle clickhouse rare') ORDER BY id DESC LIMIT 5 SETTINGS optimize_read_in_order = 1, max_threads = 1);
SELECT groupArray(id) FROM (SELECT id FROM tab_ref WHERE hasPhrase(message, 'hello clickhouse world') ORDER BY id DESC LIMIT 5);
SELECT groupArray(id) FROM (SELECT id FROM tab_win WHERE hasPhrase(message, 'hello clickhouse world') ORDER BY id DESC LIMIT 5 SETTINGS optimize_read_in_order = 1, max_threads = 1);

SELECT 'Every search walks cursors, or falls back without a codec';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, countIf(ProfileEvents['TextIndexPhraseCursorSearches'] > 0), countIf(ProfileEvents['TextIndexPhraseFallbacks'] > 0), count()
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('02346_cursors', '02346_cursors_none')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE tab_ref;
DROP TABLE tab_idx;
DROP TABLE tab_none;
DROP TABLE tab_win;
