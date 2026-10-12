-- Tags: no-parallel-replicas
-- no-parallel-replicas: the assertions below read query_log and EXPLAIN of the initiator query.
-- A text index answers match() with a pattern of letters and digits, such as 'PointerExc' inside 'NullPointerException',
-- by searching its dictionary as LIKE '%PointerExc%' does; the rows do not change. Every query is compared with use_skip_indexes = 0.

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
DROP TABLE IF EXISTS tab_fixed;
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

SELECT 'match', groupArray(id) FROM tab WHERE match(msg, 'PointerExc') SETTINGS log_comment = 'ss_match';
SELECT 'match, no index', groupArray(id) FROM tab WHERE match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

SELECT 'match, whole token', groupArray(id) FROM tab WHERE match(msg, 'Timeout') SETTINGS log_comment = 'ss_match_word';
SELECT 'match, whole token, no index', groupArray(id) FROM tab WHERE match(msg, 'Timeout') SETTINGS use_skip_indexes = 0;

SELECT 'regexp', groupArray(id) FROM tab WHERE msg REGEXP 'OfMemory' SETTINGS log_comment = 'ss_regexp';
SELECT 'regexp, no index', groupArray(id) FROM tab WHERE msg REGEXP 'OfMemory' SETTINGS use_skip_indexes = 0;

-- Not a single literal of letters and digits, so not searched in the dictionary.
SELECT 'match, alternation', groupArray(id) FROM tab WHERE match(msg, 'OutOfMemory|PointerExc') SETTINGS log_comment = 'ss_match_alternation';
SELECT 'match, alternation, no index', groupArray(id) FROM tab WHERE match(msg, 'OutOfMemory|PointerExc') SETTINGS use_skip_indexes = 0;
SELECT 'match, case-insensitive', groupArray(id) FROM tab WHERE match(msg, '(?i)pointerexc') SETTINGS log_comment = 'ss_match_ci';
SELECT 'match, case-insensitive, no index', groupArray(id) FROM tab WHERE match(msg, '(?i)pointerexc') SETTINGS use_skip_indexes = 0;
SELECT 'match, regexp', groupArray(id) FROM tab WHERE match(msg, 'Pointer.xc') SETTINGS log_comment = 'ss_match_regexp';
SELECT 'match, regexp, no index', groupArray(id) FROM tab WHERE match(msg, 'Pointer.xc') SETTINGS use_skip_indexes = 0;

-- Shorter than text_index_like_min_pattern_length.
SELECT 'match, short', groupArray(id) FROM tab WHERE match(msg, 'Poi') SETTINGS log_comment = 'ss_match_short';
SELECT 'match, short, no index', groupArray(id) FROM tab WHERE match(msg, 'Poi') SETTINGS use_skip_indexes = 0;

SELECT 'match, scan disabled', groupArray(id) FROM tab WHERE match(msg, 'PointerExc')
    SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, log_comment = 'ss_match_scan_off';
SELECT 'match, scan disabled, no index', groupArray(id) FROM tab WHERE match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

SELECT 'not', groupArray(id) FROM tab WHERE NOT match(msg, 'PointerExc');
SELECT 'not, no index', groupArray(id) FROM tab WHERE NOT match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

SELECT 'or', groupArray(id) FROM tab WHERE match(msg, 'PointerExc') OR id = 3;
SELECT 'or, no index', groupArray(id) FROM tab WHERE match(msg, 'PointerExc') OR id = 3 SETTINGS use_skip_indexes = 0;

SELECT 'select list', countIf(match(msg, 'PointerExc')) FROM tab;
SELECT 'select list, no index', countIf(match(msg, 'PointerExc')) FROM tab SETTINGS use_skip_indexes = 0;

SELECT 'The call is answered by the index alone, so it is not evaluated';

-- Columns: does the plan read a text index virtual column, does it still evaluate the original function.
SELECT 'match', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION match(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE match(msg, 'PointerExc'));

SELECT 'match, alternation', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION match(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE match(msg, 'OutOfMemory|PointerExc'));

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

SELECT 'LowCardinality', groupArray(id) FROM tab_lc WHERE match(msg, 'PointerExc') SETTINGS log_comment = 'ss_lc';
SELECT 'LowCardinality, no index', groupArray(id) FROM tab_lc WHERE match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

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

SELECT 'Nullable', groupArray(id) FROM tab_null WHERE match(msg, 'PointerExc') SETTINGS log_comment = 'ss_nullable';
SELECT 'Nullable, no index', groupArray(id) FROM tab_null WHERE match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;
SELECT 'Nullable, not', groupArray(id) FROM tab_null WHERE NOT match(msg, 'PointerExc');
SELECT 'Nullable, not, no index', groupArray(id) FROM tab_null WHERE NOT match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;
SELECT 'LowCardinality(Nullable)', groupArray(id) FROM tab_null WHERE match(lc_msg, 'PointerExc') SETTINGS log_comment = 'ss_lc_nullable';
SELECT 'LowCardinality(Nullable), no index', groupArray(id) FROM tab_null WHERE match(lc_msg, 'PointerExc') SETTINGS use_skip_indexes = 0;
SELECT 'LowCardinality(Nullable), not', groupArray(id) FROM tab_null WHERE NOT match(lc_msg, 'PointerExc');
SELECT 'LowCardinality(Nullable), not, no index', groupArray(id) FROM tab_null WHERE NOT match(lc_msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

CREATE TABLE tab_fixed
(
    id UInt32,
    msg FixedString(48),
    INDEX idx(msg) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab_fixed SELECT id, msg FROM tab;

SELECT 'FixedString', groupArray(id) FROM tab_fixed WHERE match(msg, 'PointerExc') SETTINGS log_comment = 'ss_fixed';
SELECT 'FixedString, no index', groupArray(id) FROM tab_fixed WHERE match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

-- The array tokenizer does not split a value: a literal is searched in the dictionary, any other pattern is not.
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

SELECT 'array, match', groupArray(id) FROM tab_array WHERE match(name, 'prod') SETTINGS log_comment = 'ss_array_match';
SELECT 'array, match, no index', groupArray(id) FROM tab_array WHERE match(name, 'prod') SETTINGS use_skip_indexes = 0;
SELECT 'array, regexp', groupArray(id) FROM tab_array WHERE match(name, 'svc.49') SETTINGS log_comment = 'ss_array_regexp';
SELECT 'array, regexp, no index', groupArray(id) FROM tab_array WHERE match(name, 'svc.49') SETTINGS use_skip_indexes = 0;
SELECT 'array, alternation', groupArray(id) FROM tab_array WHERE match(name, 'prod|other') SETTINGS log_comment = 'ss_array_alternation';
SELECT 'array, alternation, no index', groupArray(id) FROM tab_array WHERE match(name, 'prod|other') SETTINGS use_skip_indexes = 0;
SELECT 'array, case-insensitive', groupArray(id) FROM tab_array WHERE match(name, '(?i)axyb') SETTINGS log_comment = 'ss_array_ci';
SELECT 'array, case-insensitive, no index', groupArray(id) FROM tab_array WHERE match(name, '(?i)axyb') SETTINGS use_skip_indexes = 0;
SELECT 'array, invalid', groupArray(id) FROM tab_array WHERE match(name, '('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT 'array, invalid, no index', groupArray(id) FROM tab_array WHERE match(name, '(') SETTINGS use_skip_indexes = 0; -- { serverError CANNOT_COMPILE_REGEXP }
SELECT 'array, invalid, no row evaluated', count() FROM tab_array WHERE id > 100 AND match(name, '(');
SELECT 'array, invalid, no row evaluated, no index', count() FROM tab_array WHERE id > 100 AND match(name, '(') SETTINGS use_skip_indexes = 0;
SELECT 'array, match', countIf(explain LIKE '%\_\_text\_index\_%') > 0, countIf(explain LIKE '%FUNCTION match(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab_array WHERE match(name, 'prod'));

-- With a lower() preprocessor, the case-sensitive match is not searched in the dictionary.
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

SELECT 'lower', groupArray(id) FROM tab_lower WHERE match(msg, 'OfMemory') SETTINGS log_comment = 'ss_lower';
SELECT 'lower, no index', groupArray(id) FROM tab_lower WHERE match(msg, 'OfMemory') SETTINGS use_skip_indexes = 0;

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

SELECT 'overflow', count() FROM tab_overflow WHERE match(msg, 'PointerExc')
    SETTINGS text_index_like_max_postings_to_read = 0, log_comment = 'ss_overflow';
SELECT 'overflow, no index', count() FROM tab_overflow WHERE match(msg, 'PointerExc') SETTINGS use_skip_indexes = 0;

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

SELECT 'no index in part', groupArray(id) FROM (SELECT id FROM tab_mixed WHERE match(msg, 'PointerExc') ORDER BY id);
SELECT 'no index in part, no index', groupArray(id) FROM (SELECT id FROM tab_mixed WHERE match(msg, 'PointerExc') ORDER BY id) SETTINGS use_skip_indexes = 0;

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
DROP TABLE tab_fixed;
DROP TABLE tab_array;
DROP TABLE tab_lower;
DROP TABLE tab_overflow;
DROP TABLE tab_mixed;
