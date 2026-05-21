-- Tags: no-parallel-replicas

--- Verifies that direct read on partially materialized text indexes

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

SELECT 'Fully materialized';

DROP TABLE IF EXISTS tab_fully;
CREATE TABLE tab_fully (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id;

ALTER TABLE tab_fully DROP INDEX IF exists idx;
ALTER TABLE tab_fully ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3));

SYSTEM STOP MERGES tab_fully;

INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);
INSERT INTO tab_fully SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'o50') SETTINGS log_comment='tab_fully_hasAnyToken';
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'o50') SETTINGS log_comment='tab_fully_hasAllToken';

SYSTEM FLUSH LOGS query_log;

SELECT '-- use text index reader for all parts';
SELECT ProfileEvents['TextIndexReadPostings'] = ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_fully_hasAnyToken';
SELECT ProfileEvents['TextIndexReadPostings'] = ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_fully_hasAllToken';

SELECT '-- verify all parts have a materialized index';
SELECT count() = 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_fully' AND active AND secondary_indices_marks_bytes = 0;

SELECT 'Partially materialized';

DROP TABLE IF EXISTS tab_partially;
CREATE TABLE tab_partially (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

ALTER TABLE tab_partially DROP INDEX IF EXISTS idx;
ALTER TABLE tab_partially ADD INDEX idx(text) TYPE text(tokenizer = ngrams(3));

SYSTEM STOP MERGES tab_partially;

INSERT INTO tab_partially SELECT number, concat('hello', number % 100, ' ', 'world', number % 100) from numbers(10000);

SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'o50') SETTINGS log_comment='tab_partially_hasAnyToken';
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'o50') SETTINGS log_comment='tab_partially_hasAllToken';

SYSTEM FLUSH LOGS query_log;

SELECT '-- use text index reader for parts have a materialized index';
SELECT ProfileEvents['TextIndexReadPostings'] < ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_partially_hasAnyToken';
SELECT ProfileEvents['TextIndexReadPostings'] < ProfileEvents['SelectedPartsTotal'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = 'tab_partially_hasAllToken';

SELECT '-- verify some parts do not have a materialized index';
SELECT count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partially' AND secondary_indices_marks_bytes = 0;

DROP TABLE tab_partially;
DROP TABLE tab_fully;
