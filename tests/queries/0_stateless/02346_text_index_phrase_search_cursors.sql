-- Tags: no-parallel-replicas

SELECT 'Cursor and bitmap candidates match a scan';

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
SETTINGS allow_experimental_text_index_phrase_search = 1, text_index_posting_list_codec = 'bitpacking';

SELECT 'hello in 16 segments, needle in 2, unicorn embedded';
INSERT INTO tab_ref
SELECT number, concat(
    if(number % 3 = 0, 'hello world', 'hello clickhouse world'),
    multiIf(number % 16 = 5, ' needle clickhouse rare', number % 16 = 13, ' needle rare', ''),
    if(number IN (1000, 2000, 3000), ' unicorn clickhouse', ''))
FROM numbers(4096);

INSERT INTO tab_idx SELECT id, message FROM tab_ref;
OPTIMIZE TABLE tab_idx FINAL;

SELECT 'needle clickhouse';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'needle clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'needle clickhouse') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'needle clickhouse') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'clickhouse rare';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'clickhouse rare');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse rare') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse rare') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'needle clickhouse rare';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'needle clickhouse rare');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'needle clickhouse rare') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'needle clickhouse rare') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'world needle';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'world needle');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'world needle') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'world needle') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'clickhouse needle';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'clickhouse needle');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse needle') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse needle') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'unicorn clickhouse';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'unicorn clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'unicorn clickhouse') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'unicorn clickhouse') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'hello clickhouse';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'hello clickhouse');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'clickhouse world';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'clickhouse world');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse world') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'clickhouse world') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'world hello';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'world hello');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'world hello') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'world hello') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'hello clickhouse world';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_ref WHERE hasPhrase(message, 'hello clickhouse world');
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse world') SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '02346_cursors_leapfrog';
SELECT count(), cityHash64(arraySort(groupArray(id))) FROM tab_idx WHERE hasPhrase(message, 'hello clickhouse world') SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '02346_cursors_bruteforce';

SELECT 'Cursors run only with leapfrog';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, countIf(ProfileEvents['TextIndexPhraseCursorSearches'] > 0), count()
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('02346_cursors_leapfrog', '02346_cursors_bruteforce')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE tab_ref;
DROP TABLE tab_idx;
