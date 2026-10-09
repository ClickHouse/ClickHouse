-- Tags: no-parallel-replicas

--- Verifies that direct read on partially materialized text indexes

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
-- add_minmax_index_for_numeric_columns=0: Changes conditions for materialized index bytes

SELECT 'hasAnyToken and hasAllToken functions';

SELECT '-- fully materialized';

DROP TABLE IF EXISTS tab_fully;
CREATE TABLE tab_fully (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

ALTER TABLE tab_fully DROP INDEX IF exists idx;
ALTER TABLE tab_fully ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3), posting_list_block_size = 10000000, posting_list_codec = 'none');

SYSTEM STOP MERGES tab_fully;

INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);
INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'o50') SETTINGS log_comment='tab_fully_hasAnyToken';
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'o50') SETTINGS log_comment='tab_fully_hasAllToken';

SYSTEM FLUSH LOGS query_log;

SELECT '---- use text index reader for all parts';
SELECT ProfileEvents['TextIndexReadPostings'] = ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_fully_hasAnyToken';
SELECT ProfileEvents['TextIndexReadPostings'] = ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_fully_hasAllToken';

SELECT '---- verify all parts have a materialized index';
SELECT count() = 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_fully' AND active AND secondary_indices_marks_bytes = 0;

SELECT '-- partially materialized';

DROP TABLE IF EXISTS tab_partially;
CREATE TABLE tab_partially (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

ALTER TABLE tab_partially DROP INDEX IF EXISTS idx;
ALTER TABLE tab_partially ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3), posting_list_block_size = 10000000, posting_list_codec = 'none');

SYSTEM STOP MERGES tab_partially;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'o50') SETTINGS log_comment='tab_partially_hasAnyToken';
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'o50') SETTINGS log_comment='tab_partially_hasAllToken';

SYSTEM FLUSH LOGS query_log;

SELECT '---- use text index reader for parts have a materialized index';
SELECT ProfileEvents['TextIndexReadPostings'] < ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_partially_hasAnyToken';
SELECT ProfileEvents['TextIndexReadPostings'] < ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_partially_hasAllToken';

SELECT '---- verify some parts do not have a materialized index';
SELECT count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partially' AND secondary_indices_marks_bytes = 0;

DROP TABLE tab_partially;
DROP TABLE tab_fully;

SELECT 'hasPhrase function - without positions';

SELECT '-- fully materialized';

DROP TABLE IF EXISTS tab_fully;
CREATE TABLE tab_fully (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

ALTER TABLE tab_fully DROP INDEX IF exists idx;
ALTER TABLE tab_fully ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3), support_phrase_search = 0, posting_list_block_size = 10000000, posting_list_codec = 'none');

SYSTEM STOP MERGES tab_fully;

INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);
INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_fully WHERE hasPhrase(text, 'ello50 wor') SETTINGS log_comment='tab_fully_hasPhrase_wo_pos';

SYSTEM FLUSH LOGS query_log;

SELECT '---- use text index reader for all parts';
-- "ello50 wor" contains 8 tokens when tokenized by ngrams(3)
SELECT ProfileEvents['TextIndexReadPostings'] = (8 * (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_fully' AND active AND secondary_indices_marks_bytes > 0)) FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_fully_hasPhrase_wo_pos';

SELECT '---- verify all parts have a materialized index';
SELECT count() = 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_fully' AND active AND secondary_indices_marks_bytes = 0;

SELECT '-- partially materialized';

DROP TABLE IF EXISTS tab_partially;
CREATE TABLE tab_partially (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

ALTER TABLE tab_partially DROP INDEX IF EXISTS idx;
ALTER TABLE tab_partially ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3), support_phrase_search = 0, posting_list_block_size = 10000000, posting_list_codec = 'none');

SYSTEM STOP MERGES tab_partially;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_partially WHERE hasPhrase(text, 'ello50 wor') SETTINGS log_comment='tab_partially_hasPhrase';

SYSTEM FLUSH LOGS query_log;

SELECT '---- use text index reader for parts have a materialized index';
-- "ello50 wor" contains 8 tokens when tokenized by ngrams(3)
SELECT ProfileEvents['TextIndexReadPostings'] = (8 * (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partially' AND active AND secondary_indices_marks_bytes > 0)) FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_partially_hasPhrase';

SELECT '---- verify some parts do not have a materialized index';
SELECT count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partially' AND secondary_indices_marks_bytes = 0;

DROP TABLE tab_partially;
DROP TABLE tab_fully;

SELECT 'hasPhrase function - with positions';

SELECT '-- fully materialized';

DROP TABLE IF EXISTS tab_fully;
CREATE TABLE tab_fully (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, allow_experimental_text_index_phrase_search = 1;

ALTER TABLE tab_fully DROP INDEX IF exists idx;
ALTER TABLE tab_fully ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1, posting_list_block_size = 10000000, posting_list_codec = 'none');

SYSTEM STOP MERGES tab_fully;

INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);
INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_fully WHERE hasPhrase(text, 'ello50 wor') SETTINGS log_comment='tab_fully_hasPhrase_w_pos';

SYSTEM FLUSH LOGS query_log;

SELECT '---- use text index reader for all parts';
-- "ello50 wor" contains 8 tokens when tokenized by ngrams(3)
SELECT ProfileEvents['TextIndexReadPostings'] = (8 * (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_fully' AND active AND secondary_indices_marks_bytes > 0)) FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_fully_hasPhrase_w_pos';

SELECT '---- verify all parts have a materialized index';
SELECT count() = 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_fully' AND active AND secondary_indices_marks_bytes = 0;

SELECT '-- partially materialized';

DROP TABLE IF EXISTS tab_partially;
CREATE TABLE tab_partially (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

ALTER TABLE tab_partially DROP INDEX IF EXISTS idx;
ALTER TABLE tab_partially ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1, posting_list_block_size = 10000000, posting_list_codec = 'none');

SYSTEM STOP MERGES tab_partially;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_partially WHERE hasPhrase(text, 'ello50 wor') SETTINGS log_comment='tab_partially_hasPhrase_pos';

SYSTEM FLUSH LOGS query_log;

SELECT '---- use text index reader for parts have a materialized index';
-- "ello50 wor" contains 8 tokens when tokenized by ngrams(3)
SELECT ProfileEvents['TextIndexReadPostings'] = (8 * (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partially' AND active AND secondary_indices_marks_bytes > 0)) FROM system.query_log WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_partially_hasPhrase_pos';

SELECT '---- verify some parts do not have a materialized index';
SELECT count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partially' AND secondary_indices_marks_bytes = 0;

DROP TABLE tab_partially;
DROP TABLE tab_fully;

SELECT 'hasPhrase function - with positions - part merge';

DROP TABLE IF EXISTS tab_merge;
CREATE TABLE tab_merge (
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
Engine = MergeTree()
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, allow_experimental_text_index_phrase_search = 1;

SYSTEM STOP MERGES tab_merge;

INSERT INTO tab_merge VALUES
    (1, 'quick brown fox'),
    (2, 'brown fox jumps'),
    (3, 'lazy fox runs really really fast');

INSERT INTO tab_merge VALUES
    (4, 'quick brown dog'),
    (5, 'the brown fox'),
    (6, 'go go stop');

-- 'alpha' at token position 31 (bucket 0), 'beta' at 32 (bucket 1): the phrase crosses a roaringish bucket boundary.
INSERT INTO tab_merge SELECT 7, concat(repeat('w ', 31), 'alpha beta');

SELECT '-- before merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_merge' AND active;
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'quick brown');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'really really');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'go go');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'alpha beta');

SYSTEM START MERGES tab_merge;
OPTIMIZE TABLE tab_merge FINAL;

SELECT '-- after merge (must match before)';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_merge' AND active;
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'quick brown');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'brown fox');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'really really');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'go go');
SELECT arraySort(groupArray(id)) FROM tab_merge WHERE hasPhrase(message, 'alpha beta');

DROP TABLE tab_merge;
