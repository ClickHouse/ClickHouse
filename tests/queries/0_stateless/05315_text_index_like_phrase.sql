-- Tags: no-parallel-replicas
-- no-parallel-replicas: the assertions below read ProfileEvents of the initiator query.
-- Tests that a text index skips granules for a LIKE or ILIKE phrase of several words, such as '%java heap%',
-- and reads fewer dictionary blocks for a case-insensitive prefix such as ILIKE 'ab01%', and that neither
-- changes which rows are returned. Every query is compared against the same query with use_skip_indexes = 0.

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

-- A word containing 'k' or starting with 's' is not searched; here no other word is left, so nothing is pruned.
SELECT 'ilike, only word with k', groupArray(id) FROM tab WHERE message ILIKE '%java kafka%' SETTINGS log_comment = '05315_ilike_k';
SELECT 'ilike, only word with k, no index', groupArray(id) FROM tab WHERE message ILIKE '%java kafka%' SETTINGS use_skip_indexes = 0;
SELECT 'ilike, only word with s', groupArray(id) FROM tab WHERE message ILIKE '%java sunrise%' SETTINGS log_comment = '05315_ilike_s';
SELECT 'ilike, only word with s, no index', groupArray(id) FROM tab WHERE message ILIKE '%java sunrise%' SETTINGS use_skip_indexes = 0;

-- Both 'heap' and 'ab001' can be searched; 'ab001' is chosen, as more of its leading characters narrow the dictionary scan.
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

-- A case-insensitive prefix reads only the dictionary blocks holding the case variants of 'ab01'.
SELECT 'ilike prefix', groupArray(id) FROM tab WHERE message ILIKE 'AB01%' SETTINGS log_comment = '05315_ilike_prefix';
SELECT 'ilike prefix, no index', groupArray(id) FROM tab WHERE message ILIKE 'AB01%' SETTINGS use_skip_indexes = 0;

-- The same prefix together with an infix pattern, which reads every dictionary block.
SELECT 'every dictionary block', groupArray(id) FROM tab WHERE message ILIKE 'AB01%' OR message ILIKE '%ab01%' SETTINGS log_comment = '05315_ilike_all_blocks';

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
                      '05315_ilike_kelvin', '05315_ilike_k', '05315_ilike_s', '05315_like_and_ilike', '05315_like_budget',
                      '05315_ilike_prefix', '05315_ilike_all_blocks')
ORDER BY log_comment;

-- Every row is a granule, so a phrase query reads exactly the rows holding a token that matches its searched word.
SELECT log_comment, read_rows
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05315_like', '05315_like_punctuation', '05315_ilike', '05315_ilike_three', '05315_ilike_kelvin',
                      '05315_ilike_two_words')
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

SELECT 'Array tokenizer';

DROP TABLE IF EXISTS tab_array;

-- The array tokenizer keeps the whole value as one token, and the index alone answers an ILIKE prefix,
-- so a token in any letter case must be found. The 'zz' tokens sort between the ASCII and the non-ASCII
-- tokens, so the dictionary block holding the latter holds no token starting with 'sunr'.
CREATE TABLE tab_array
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_array SELECT number + 1, concat('ab', leftPad(toString(number), 3, '0')) FROM numbers(32);
INSERT INTO tab_array VALUES
    (101, 'AB010x'), (102, 'Ab011'), (103, 'aB012'), (104, concat(unhex('C5BF'), 'unrise sunrise')),
    (105, 'sunrise'), (106, 'SUNRISE'), (107, concat(unhex('E284AA'), 'elvin kelvin')), (108, 'kelvin');
INSERT INTO tab_array SELECT 201 + number, concat('zz', leftPad(toString(number), 3, '0')) FROM numbers(8);
OPTIMIZE TABLE tab_array FINAL;

SELECT 'ilike prefix', groupArray(id) FROM tab_array WHERE message ILIKE 'AB01%' SETTINGS log_comment = '05315_array_prefix';
SELECT 'ilike prefix, no index', groupArray(id) FROM tab_array WHERE message ILIKE 'AB01%' SETTINGS use_skip_indexes = 0;

SELECT 'ilike exact', groupArray(id) FROM tab_array WHERE message ILIKE 'ab011' SETTINGS log_comment = '05315_array_exact';
SELECT 'ilike exact, no index', groupArray(id) FROM tab_array WHERE message ILIKE 'ab011' SETTINGS use_skip_indexes = 0;

-- ILIKE treats U+017F as 's' here, so a prefix starting with 's' reads every dictionary block.
SELECT 'ilike prefix with s', groupArray(id) FROM tab_array WHERE message ILIKE 'sunr%' SETTINGS log_comment = '05315_array_s';
SELECT 'ilike prefix with s, no index', groupArray(id) FROM tab_array WHERE message ILIKE 'sunr%' SETTINGS use_skip_indexes = 0;

SELECT 'ilike prefix with k', groupArray(id) FROM tab_array WHERE message ILIKE 'kelvi%';
SELECT 'ilike prefix with k, no index', groupArray(id) FROM tab_array WHERE message ILIKE 'kelvi%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment IN ('05315_array_prefix', '05315_array_exact', '05315_array_s')
ORDER BY log_comment;

DROP TABLE tab_array;
