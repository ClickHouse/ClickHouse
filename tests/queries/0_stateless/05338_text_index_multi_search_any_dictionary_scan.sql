-- Tags: no-parallel-replicas
-- no-parallel-replicas: the assertions below read query_log and EXPLAIN of the initiator query.
-- Tests that a text index answers multiSearchAny, multiSearchAnyUTF8 and multiSearchAnyCaseInsensitive[UTF8] with one
-- needle as LIKE '%needle%' (ILIKE), for a needle inside one token such as 'PointerExc', and that the rows do not change.
-- Calls with several needles keep reading the column. Every query is compared with use_skip_indexes = 0.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET text_index_like_max_postings_to_read = 100000;
SET use_query_condition_cache = 0;
SET explain_query_plan_default = 'legacy';
SET max_threads = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_lc;
DROP TABLE IF EXISTS tab_null;
DROP TABLE IF EXISTS tab_array;
DROP TABLE IF EXISTS tab_lower;
DROP TABLE IF EXISTS tab_overflow;
DROP TABLE IF EXISTS tab_mixed;

-- Every row is a granule of its own, so a row is read only if the index keeps its granule.
CREATE TABLE tab
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (1, 'java.lang.NullPointerException at Main'), (2, 'java.lang.OutOfMemoryError: Java heap space'),
    (3, 'connection Timeout'), (4, 'all good'), (5, 'nullpointerexception in lower case'),
    (6, concat(unhex('E284AA'), 'elvin')), (7, concat(unhex('C4B0'), 'stanbul')), (8, concat(unhex('C5BF'), 'unrise')),
    (9, 'KELVIN'), (10, 'sunrise'), (11, 'Pointer Exc'), (12, 'Poi% value'), (13, '');

SELECT 'multiSearchAny', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS log_comment = 'ss_msa';
SELECT 'multiSearchAny, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;

SELECT 'multiSearchAnyUTF8', groupArray(id) FROM tab WHERE multiSearchAnyUTF8(msg, ['PointerExc']) SETTINGS log_comment = 'ss_msa_utf8';
SELECT 'multiSearchAnyUTF8, no index', groupArray(id) FROM tab WHERE multiSearchAnyUTF8(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;

SELECT 'multiSearchAnyCaseInsensitive', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY']) SETTINGS log_comment = 'ss_msa_ci';
SELECT 'multiSearchAnyCaseInsensitive, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY']) SETTINGS use_skip_indexes = 0;

-- Some characters fold to an ASCII letter, such as U+212A to 'k': a needle with such a letter is not searched in the dictionary.
SELECT 'multiSearchAnyCaseInsensitiveUTF8', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['STANBUL']) SETTINGS log_comment = 'ss_msa_ci_utf8';
SELECT 'multiSearchAnyCaseInsensitiveUTF8, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['STANBUL']) SETTINGS use_skip_indexes = 0;

SELECT 'elvin', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['elvin']) SETTINGS log_comment = 'ss_fold_elvin';
SELECT 'elvin, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['elvin']) SETTINGS use_skip_indexes = 0;
SELECT 'stanbul', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['stanbul']) SETTINGS log_comment = 'ss_fold_stanbul';
SELECT 'stanbul, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['stanbul']) SETTINGS use_skip_indexes = 0;
SELECT 'istanbul', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['istanbul']) SETTINGS log_comment = 'ss_fold_istanbul';
SELECT 'istanbul, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['istanbul']) SETTINGS use_skip_indexes = 0;
SELECT 'unrise', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['unrise']) SETTINGS log_comment = 'ss_fold_unrise';
SELECT 'unrise, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['unrise']) SETTINGS use_skip_indexes = 0;
SELECT 'sunrise', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['sunrise']) SETTINGS log_comment = 'ss_fold_sunrise';
SELECT 'sunrise, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['sunrise']) SETTINGS use_skip_indexes = 0;
SELECT 'KELV', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['KELV']) SETTINGS log_comment = 'ss_fold_kelv';
SELECT 'KELV, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['KELV']) SETTINGS use_skip_indexes = 0;

SELECT 'no needles', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, CAST([], 'Array(String)'));
SELECT 'no needles, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, CAST([], 'Array(String)')) SETTINGS use_skip_indexes = 0;

-- Several needles are not searched in the dictionary.
SELECT 'multiSearchAny, 2 needles', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc', 'OfMemory']) SETTINGS log_comment = 'ss_msa_2';
SELECT 'multiSearchAny, 2 needles, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc', 'OfMemory']) SETTINGS use_skip_indexes = 0;
SELECT 'multiSearchAnyCaseInsensitive, 2 needles', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY', 'connect']) SETTINGS log_comment = 'ss_msa_ci_2';
SELECT 'multiSearchAnyCaseInsensitive, 2 needles, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY', 'connect']) SETTINGS use_skip_indexes = 0;

SELECT 'not', groupArray(id) FROM tab WHERE NOT multiSearchAny(msg, ['PointerExc']);
SELECT 'not, no index', groupArray(id) FROM tab WHERE NOT multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;

SELECT 'or', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc']) OR id = 3;
SELECT 'or, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc']) OR id = 3 SETTINGS use_skip_indexes = 0;

SELECT 'select list', countIf(multiSearchAny(msg, ['PointerExc'])) FROM tab;
SELECT 'select list, no index', countIf(multiSearchAny(msg, ['PointerExc'])) FROM tab SETTINGS use_skip_indexes = 0;

-- Not eligible needles: shorter than text_index_like_min_pattern_length, with a separator, with a LIKE wildcard.
SELECT 'short needle', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['Poi']) SETTINGS log_comment = 'ss_short';
SELECT 'short needle, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['Poi']) SETTINGS use_skip_indexes = 0;
SELECT 'separator', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['Pointer Exc']) SETTINGS log_comment = 'ss_separator';
SELECT 'separator, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['Pointer Exc']) SETTINGS use_skip_indexes = 0;
SELECT 'wildcard', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['Poi%']) SETTINGS log_comment = 'ss_wildcard';
SELECT 'wildcard, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['Poi%']) SETTINGS use_skip_indexes = 0;

-- With use_text_index_like_evaluation_by_dictionary_scan = 0 a needle inside one token does not use the index.
SET use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'scan off', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS log_comment = 'ss_scan_off';
SELECT 'scan off, no index', groupArray(id) FROM tab WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'scan off, case-insensitive', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY']) SETTINGS log_comment = 'ss_scan_off_ci';
SELECT 'scan off, case-insensitive, no index', groupArray(id) FROM tab WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY']) SETTINGS use_skip_indexes = 0;
SELECT 'scan off', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION multiSearchAny(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE multiSearchAny(msg, ['PointerExc']));
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

-- optimize_or_like_chain rewrites the chain into multiSearchAny with 4 needles.
SELECT 'like chain', groupArray(id) FROM tab
    WHERE msg LIKE '%OutOfMemory%' OR msg LIKE '%PointerExc%' OR msg LIKE '%imeou%' OR msg LIKE '%heap%'
    SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_substrings = 4;
SELECT 'like chain, not rewritten', groupArray(id) FROM tab
    WHERE msg LIKE '%OutOfMemory%' OR msg LIKE '%PointerExc%' OR msg LIKE '%imeou%' OR msg LIKE '%heap%'
    SETTINGS optimize_or_like_chain = 0;

SELECT 'The call is answered by the index alone, so it is not evaluated';

-- Columns: does the plan read a text index virtual column, does it still evaluate the original function.
SELECT 'multiSearchAny', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION multiSearchAny(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE multiSearchAny(msg, ['PointerExc']));

SELECT 'multiSearchAnyCaseInsensitiveUTF8', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION multiSearchAnyCaseInsensitiveUTF8(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE multiSearchAnyCaseInsensitiveUTF8(msg, ['OUTOFMEMORY']));

SELECT 'like chain', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION multiSearchAny(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab
    WHERE msg LIKE '%OutOfMemory%' OR msg LIKE '%PointerExc%' OR msg LIKE '%imeou%' OR msg LIKE '%heap%'
    SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_substrings = 4);

SELECT 'Other column types';

CREATE TABLE tab_lc
(
    id UInt32,
    msg LowCardinality(String),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_lc SELECT id, msg FROM tab;

SELECT 'LowCardinality', groupArray(id) FROM tab_lc WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS log_comment = 'ss_lc';
SELECT 'LowCardinality, no index', groupArray(id) FROM tab_lc WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;

-- A NULL value gives a NULL that the virtual column cannot carry, so a Nullable column keeps the original predicate.
CREATE TABLE tab_null
(
    id UInt32,
    msg Nullable(String),
    lc_msg LowCardinality(Nullable(String)),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1,
    INDEX idx_lc(lc_msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_null SELECT id, if(id % 4 = 0, NULL, msg), if(id % 4 = 0, NULL, msg) FROM tab;

SELECT 'Nullable', groupArray(id) FROM tab_null WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS log_comment = 'ss_nullable';
SELECT 'Nullable, no index', groupArray(id) FROM tab_null WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'Nullable, not', groupArray(id) FROM tab_null WHERE NOT multiSearchAny(msg, ['PointerExc']);
SELECT 'Nullable, not, no index', groupArray(id) FROM tab_null WHERE NOT multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'LowCardinality(Nullable)', groupArray(id) FROM tab_null WHERE multiSearchAny(lc_msg, ['PointerExc']) SETTINGS log_comment = 'ss_lc_nullable';
SELECT 'LowCardinality(Nullable), no index', groupArray(id) FROM tab_null WHERE multiSearchAny(lc_msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;
SELECT 'LowCardinality(Nullable), not', groupArray(id) FROM tab_null WHERE NOT multiSearchAny(lc_msg, ['PointerExc']);
SELECT 'LowCardinality(Nullable), not, no index', groupArray(id) FROM tab_null WHERE NOT multiSearchAny(lc_msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;

-- The array tokenizer does not split a value, so any needle is searched in the dictionary, with LIKE wildcards escaped.
CREATE TABLE tab_array
(
    id UInt32,
    name String,
    INDEX idx(name) TYPE text(tokenizer = array) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_array VALUES (1, 'aa%bb-svc'), (2, 'aaXbb-svc'), (3, 'xx_yy-svc'), (4, 'xxzyy-svc'), (5, 'svc-49-prod'), (6, 'svc-4'), (7, 'other'), (8, 'AXYB');

SELECT 'array, multiSearchAny', groupArray(id) FROM tab_array WHERE multiSearchAny(name, ['aa%bb']) SETTINGS log_comment = 'ss_array_msa';
SELECT 'array, multiSearchAny, no index', groupArray(id) FROM tab_array WHERE multiSearchAny(name, ['aa%bb']) SETTINGS use_skip_indexes = 0;
SELECT 'array, underscore', groupArray(id) FROM tab_array WHERE multiSearchAny(name, ['xx_yy']) SETTINGS log_comment = 'ss_array_msa_underscore';
SELECT 'array, underscore, no index', groupArray(id) FROM tab_array WHERE multiSearchAny(name, ['xx_yy']) SETTINGS use_skip_indexes = 0;
SELECT 'array, separator', groupArray(id) FROM tab_array WHERE multiSearchAny(name, ['svc-49']) SETTINGS log_comment = 'ss_array_msa_separator';
SELECT 'array, separator, no index', groupArray(id) FROM tab_array WHERE multiSearchAny(name, ['svc-49']) SETTINGS use_skip_indexes = 0;
SELECT 'array, multiSearchAnyCaseInsensitive', groupArray(id) FROM tab_array WHERE multiSearchAnyCaseInsensitive(name, ['axyb']) SETTINGS log_comment = 'ss_array_msa_ci';
SELECT 'array, multiSearchAnyCaseInsensitive, no index', groupArray(id) FROM tab_array WHERE multiSearchAnyCaseInsensitive(name, ['axyb']) SETTINGS use_skip_indexes = 0;
SELECT 'array, multiSearchAny', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION multiSearchAny(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_array WHERE multiSearchAny(name, ['aa%bb']));

-- With a lower() preprocessor, only the case-insensitive functions are searched in the dictionary.
CREATE TABLE tab_lower
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(msg)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_lower SELECT id, msg FROM tab;

SELECT 'lower, multiSearchAnyCaseInsensitive', groupArray(id) FROM tab_lower WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY']) SETTINGS log_comment = 'ss_lower_ci';
SELECT 'lower, multiSearchAnyCaseInsensitive, no index', groupArray(id) FROM tab_lower WHERE multiSearchAnyCaseInsensitive(msg, ['OUTOFMEMORY']) SETTINGS use_skip_indexes = 0;
SELECT 'lower, multiSearchAny', groupArray(id) FROM tab_lower WHERE multiSearchAny(msg, ['OfMemory']) SETTINGS log_comment = 'ss_lower';
SELECT 'lower, multiSearchAny, no index', groupArray(id) FROM tab_lower WHERE multiSearchAny(msg, ['OfMemory']) SETTINGS use_skip_indexes = 0;

SELECT 'Fallbacks';

-- A token in more rows than a posting list can embed: the scan gives up at once and the original call is evaluated.
CREATE TABLE tab_overflow
(
    id UInt32,
    msg String,
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_overflow SELECT number, if(number % 3 = 0, 'java.lang.NullPointerException', 'all good') FROM numbers(3000);

SELECT 'overflow', count() FROM tab_overflow WHERE multiSearchAny(msg, ['PointerExc'])
    SETTINGS text_index_like_max_postings_to_read = 0, log_comment = 'ss_overflow';
SELECT 'overflow, no index', count() FROM tab_overflow WHERE multiSearchAny(msg, ['PointerExc']) SETTINGS use_skip_indexes = 0;

-- A part from before ADD INDEX has no index, its rows are filtered by the original call.
CREATE TABLE tab_mixed
(
    id UInt32,
    msg String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_mixed VALUES (1, 'java.lang.NullPointerException'), (2, 'all good');
ALTER TABLE tab_mixed ADD INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1;
INSERT INTO tab_mixed VALUES (3, 'java.lang.NullPointerException'), (4, 'all good');

SELECT 'no index in part', groupArray(id) FROM (SELECT id FROM tab_mixed WHERE multiSearchAny(msg, ['PointerExc']) ORDER BY id);
SELECT 'no index in part, no index', groupArray(id) FROM (SELECT id FROM tab_mixed WHERE multiSearchAny(msg, ['PointerExc']) ORDER BY id) SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

-- Columns: are rows skipped by the index (every row is a granule).
SELECT log_comment, read_rows < if(log_comment LIKE 'ss\_array%', 8, 13)
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE 'ss\_%' AND log_comment != 'ss_overflow'
ORDER BY log_comment;

SELECT 'overflow, the scan gave up', ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'ss_overflow';

DROP TABLE tab;
DROP TABLE tab_lc;
DROP TABLE tab_null;
DROP TABLE tab_array;
DROP TABLE tab_lower;
DROP TABLE tab_overflow;
DROP TABLE tab_mixed;
