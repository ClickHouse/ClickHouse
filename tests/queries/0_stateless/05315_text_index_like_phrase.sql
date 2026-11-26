-- Tags: no-parallel-replicas
-- no-parallel-replicas: the assertions below read ProfileEvents of the initiator query.
-- Tests that a text index skips granules for a LIKE or ILIKE phrase of several words, such as '%java heap%',
-- and that this does not change which rows are returned. Every query is compared against the same query
-- with use_skip_indexes = 0.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET text_index_like_max_postings_to_read = 100000;
SET use_query_condition_cache = 0;
SET optimize_rewrite_like_perfect_affix = 0;
SET max_threads = 1;
SET log_queries = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS tab;

-- Every row is a granule of its own, so a row is returned only if the index keeps its granule.
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number + 1, concat('ab', leftPad(toString(number), 3, '0')) FROM numbers(32);
INSERT INTO tab VALUES
    (101, 'xjava heap'), (102, 'java heapdump'), (103, 'java heap'), (104, 'heap java'),
    (105, 'Java HEAP'), (106, 'jAvA hEaPs'), (107, 'java.lang.Error'), (108, 'Java Heap Space'),
    (109, concat(unhex('E284AA'), 'elvin heap')), (110, concat('java ', unhex('E284AA'), 'afka')), (111, 'JAVA SUNRISE'), (112, 'java sunrise'),
    (113, 'Kelvin Heap'), (114, 'java Kafka'), (115, concat('java ', unhex('C5BF'), 'unrise')), (116, 'ab999');
-- 16 rows of the token 'budget', more than a token can embed, so that the postings budget below is charged.
INSERT INTO tab SELECT 201 + number, 'error budget' FROM numbers(16);
OPTIMIZE TABLE tab FINAL;

-- The first word may end a longer token and the last may start one: 'xjava heap' and 'java heapdump' match.
SELECT 'like', groupArray(id) FROM tab WHERE message LIKE '%java heap%' SETTINGS log_comment = '05315_like';
SELECT 'like, no index', groupArray(id) FROM tab WHERE message LIKE '%java heap%' SETTINGS use_skip_indexes = 0;

SELECT 'like, punctuation', groupArray(id) FROM tab WHERE message LIKE '%java.lang%' SETTINGS log_comment = '05315_like_punctuation';
SELECT 'like, punctuation, no index', groupArray(id) FROM tab WHERE message LIKE '%java.lang%' SETTINGS use_skip_indexes = 0;

-- The last word is shorter than text_index_like_min_pattern_length, so nothing is pruned.
SELECT 'like, short last word', groupArray(id) FROM tab WHERE message LIKE '%java hea%' SETTINGS log_comment = '05315_like_short';
SELECT 'like, short last word, no index', groupArray(id) FROM tab WHERE message LIKE '%java hea%' SETTINGS use_skip_indexes = 0;

SELECT 'ilike', groupArray(id) FROM tab WHERE message ILIKE '%JAVA HEAP%' SETTINGS log_comment = '05315_ilike';
SELECT 'ilike, no index', groupArray(id) FROM tab WHERE message ILIKE '%JAVA HEAP%' SETTINGS use_skip_indexes = 0;

-- 'heap' is a whole token here. Words starting with 's' are not searched.
SELECT 'ilike, three words', groupArray(id) FROM tab WHERE message ILIKE '%java heap space%' SETTINGS log_comment = '05315_ilike_three';
SELECT 'ilike, three words, no index', groupArray(id) FROM tab WHERE message ILIKE '%java heap space%' SETTINGS use_skip_indexes = 0;

-- Only 'heap' is searched, so the first word may contain 'k'.
SELECT 'ilike, kelvin sign', groupArray(id) FROM tab WHERE message ILIKE '%Kelvin heap%' SETTINGS log_comment = '05315_ilike_kelvin';
SELECT 'ilike, kelvin sign, no index', groupArray(id) FROM tab WHERE message ILIKE '%Kelvin heap%' SETTINGS use_skip_indexes = 0;

-- A word starting with 'k' or 's' is not searched; here no other word is left, so nothing is pruned.
SELECT 'ilike, only word with k', groupArray(id) FROM tab WHERE message ILIKE '%java kafka%' SETTINGS log_comment = '05315_ilike_k';
SELECT 'ilike, only word with k, no index', groupArray(id) FROM tab WHERE message ILIKE '%java kafka%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike, only word with s', groupArray(id) FROM tab WHERE message ILIKE '%java sunrise%' SETTINGS log_comment = '05315_ilike_s';
SELECT 'ilike, only word with s, no index', groupArray(id) FROM tab WHERE message ILIKE '%java sunrise%' SETTINGS use_skip_indexes = 0;

-- Both 'heap' and 'ab001' can be searched; the longer 'ab001' is chosen.
SELECT 'ilike, two searchable words', groupArray(id) FROM tab WHERE message ILIKE '%java heap ab001%' SETTINGS log_comment = '05315_ilike_two_words';
SELECT 'ilike, two searchable words, no index', groupArray(id) FROM tab WHERE message ILIKE '%java heap ab001%' SETTINGS use_skip_indexes = 0;

SELECT 'not like', count() FROM tab WHERE NOT (message LIKE '%java heap%');
SELECT 'not like, no index', count() FROM tab WHERE NOT (message LIKE '%java heap%') SETTINGS use_skip_indexes = 0;
SELECT 'not ilike', count() FROM tab WHERE NOT (message ILIKE '%java heap%');
SELECT 'not ilike, no index', count() FROM tab WHERE NOT (message ILIKE '%java heap%') SETTINGS use_skip_indexes = 0;

SELECT 'like and ilike', groupArray(id) FROM tab WHERE message LIKE '%java heap%' AND message ILIKE '%JAVA HEAPDUMP%' SETTINGS log_comment = '05315_like_and_ilike';
SELECT 'like and ilike, no index', groupArray(id) FROM tab WHERE message LIKE '%java heap%' AND message ILIKE '%JAVA HEAPDUMP%' SETTINGS use_skip_indexes = 0;

SELECT 'postings budget exhausted', groupArray(id) FROM tab WHERE message LIKE '%error budget%' SETTINGS log_comment = '05315_like_budget', text_index_like_max_postings_to_read = 0;
SELECT 'postings budget exhausted, no index', groupArray(id) FROM tab WHERE message LIKE '%error budget%' SETTINGS use_skip_indexes = 0;

-- The phrase is not searched along with another pattern, so its postings cannot exhaust the budget of '%lang%'.
SELECT 'like and phrase, postings budget', groupArray(id) FROM tab WHERE message LIKE '%lang%' AND message LIKE '%error budget%' SETTINGS log_comment = '05315_like_and_budget', text_index_like_max_postings_to_read = 0;
SELECT 'like and phrase, postings budget, no index', groupArray(id) FROM tab WHERE message LIKE '%lang%' AND message LIKE '%error budget%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS pattern_scan_discarded,
    read_rows < (SELECT count() FROM tab) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05315_like', '05315_like_punctuation', '05315_like_short', '05315_ilike', '05315_ilike_three',
                      '05315_ilike_kelvin', '05315_ilike_k', '05315_ilike_s', '05315_like_and_ilike', '05315_like_budget')
ORDER BY log_comment;

-- Every row is a granule, so a phrase query reads exactly the rows holding a token that matches its searched word.
SELECT log_comment, read_rows
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05315_like', '05315_like_punctuation', '05315_ilike', '05315_ilike_three', '05315_ilike_kelvin',
                      '05315_ilike_two_words', '05315_like_and_budget')
ORDER BY log_comment;

DROP TABLE tab;

SELECT 'Nullable';

DROP TABLE IF EXISTS tab_nullable;

CREATE TABLE tab_nullable
(
    id UInt32,
    message Nullable(String),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_nullable SELECT number + 1, if(number % 4 = 0, NULL, concat('ab', leftPad(toString(number), 3, '0'))) FROM numbers(16);
INSERT INTO tab_nullable VALUES (101, 'java heap'), (102, NULL), (103, 'JAVA HEAP'), (104, 'heap');
OPTIMIZE TABLE tab_nullable FINAL;

SELECT 'like', groupArray(id) FROM tab_nullable WHERE message LIKE '%java heap%' SETTINGS log_comment = '05315_nullable_like';
SELECT 'like, no index', groupArray(id) FROM tab_nullable WHERE message LIKE '%java heap%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike', groupArray(id) FROM tab_nullable WHERE message ILIKE '%java heap%' SETTINGS log_comment = '05315_nullable_ilike';
SELECT 'ilike, no index', groupArray(id) FROM tab_nullable WHERE message ILIKE '%java heap%' SETTINGS use_skip_indexes = 0;
SELECT 'not like', count() FROM tab_nullable WHERE NOT (message LIKE '%java heap%');
SELECT 'not like, no index', count() FROM tab_nullable WHERE NOT (message LIKE '%java heap%') SETTINGS use_skip_indexes = 0;
SELECT 'not ilike', count() FROM tab_nullable WHERE NOT (message ILIKE '%java heap%');
SELECT 'not ilike, no index', count() FROM tab_nullable WHERE NOT (message ILIKE '%java heap%') SETTINGS use_skip_indexes = 0;

-- A part holding a NULL and a phrase word whose postings exhaust the budget: '%java%' must not fall back to row evaluation.
INSERT INTO tab_nullable SELECT 201 + number, if(number = 0, NULL, 'error budget') FROM numbers(17);
SELECT 'like or phrase, postings budget', count() FROM tab_nullable WHERE message LIKE '%java%' OR message ILIKE '%error budget%' SETTINGS text_index_like_max_postings_to_read = 0;
SELECT 'like or phrase, postings budget, no index', count() FROM tab_nullable WHERE message LIKE '%java%' OR message ILIKE '%error budget%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    read_rows < (SELECT count() FROM tab_nullable) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment IN ('05315_nullable_like', '05315_nullable_ilike')
ORDER BY log_comment;

DROP TABLE tab_nullable;

SELECT 'Preprocessor';

DROP TABLE IF EXISTS tab_lower;

CREATE TABLE tab_lower
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message), dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_lower SELECT number + 1, concat('ab', leftPad(toString(number), 3, '0')) FROM numbers(16);
INSERT INTO tab_lower VALUES (101, 'java heap'), (102, 'JAVA HEAP'), (103, 'Java Heapdump'), (104, 'heap');
OPTIMIZE TABLE tab_lower FINAL;

-- LIKE is case-sensitive, so a lowercased index does not answer it and nothing is pruned.
SELECT 'like', groupArray(id) FROM tab_lower WHERE message LIKE '%java heap%' SETTINGS log_comment = '05315_lower_like';
SELECT 'like, no index', groupArray(id) FROM tab_lower WHERE message LIKE '%java heap%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike', groupArray(id) FROM tab_lower WHERE message ILIKE '%JAVA HEAP%' SETTINGS log_comment = '05315_lower_ilike';
SELECT 'ilike, no index', groupArray(id) FROM tab_lower WHERE message ILIKE '%JAVA HEAP%' SETTINGS use_skip_indexes = 0;
SELECT 'not ilike', count() FROM tab_lower WHERE NOT (message ILIKE '%java heap%');
SELECT 'not ilike, no index', count() FROM tab_lower WHERE NOT (message ILIKE '%java heap%') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    read_rows < (SELECT count() FROM tab_lower) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment IN ('05315_lower_like', '05315_lower_ilike')
ORDER BY log_comment;

DROP TABLE tab_lower;

SELECT 'Many tokens';

DROP TABLE IF EXISTS tab_many;

CREATE TABLE tab_many
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- The searched word 'user' of '%error user%' starts 64 distinct tokens, each in one row, so their postings are embedded.
INSERT INTO tab_many SELECT number + 1, concat('error user', toString(number)) FROM numbers(64);
INSERT INTO tab_many VALUES (101, 'error budget');
OPTIMIZE TABLE tab_many FINAL;

-- Every matched token counts against the postings budget, so a budget of 50 stops the search and nothing is pruned.
SELECT 'like', count() FROM tab_many WHERE message LIKE '%error user%' SETTINGS log_comment = '05315_many_like', text_index_like_max_postings_to_read = 50;
SELECT 'like, no index', count() FROM tab_many WHERE message LIKE '%error user%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike', count() FROM tab_many WHERE message ILIKE '%ERROR USER%' SETTINGS log_comment = '05315_many_ilike', text_index_like_max_postings_to_read = 50, use_text_index_pattern_bypass_cache = 1;
SELECT 'ilike, no index', count() FROM tab_many WHERE message ILIKE '%ERROR USER%' SETTINGS use_skip_indexes = 0;
-- 'USER%' searched on its own is charged by large postings only, so it does not reuse the bailout the phrase cached.
SELECT 'ilike prefix', count() FROM tab_many WHERE message ILIKE 'USER%' SETTINGS log_comment = '05315_many_ilike_prefix', text_index_like_max_postings_to_read = 50, use_text_index_pattern_bypass_cache = 1, query_plan_text_index_add_hint = 0;
SELECT 'ilike prefix, no index', count() FROM tab_many WHERE message ILIKE 'USER%' SETTINGS use_skip_indexes = 0;
SELECT 'like, budget of all tokens', count() FROM tab_many WHERE message LIKE '%error user%' SETTINGS log_comment = '05315_many_like_in_budget', text_index_like_max_postings_to_read = 64;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS pattern_scan_discarded,
    read_rows < (SELECT count() FROM tab_many) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment IN ('05315_many_like', '05315_many_ilike', '05315_many_ilike_prefix', '05315_many_like_in_budget')
ORDER BY log_comment;

DROP TABLE tab_many;
