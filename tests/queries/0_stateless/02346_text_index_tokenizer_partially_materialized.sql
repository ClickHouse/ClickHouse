--- Verifies that tokenizer is properly passed to supported functions when a text index is partially materialized.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET enable_analyzer  = 0;

SELECT 'Fully materialized';

DROP TABLE IF EXISTS tab_fully;
CREATE TABLE tab_fully (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id;

ALTER TABLE tab_fully ADD INDEX idx(text) TYPE text(tokenizer = splitByString([' ', '::']));

SYSTEM STOP MERGES tab_fully;

INSERT INTO tab_fully SELECT number, 'hello::world' from numbers(10000);
INSERT INTO tab_fully SELECT number, 'hello world' from numbers(10000);

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'hello') SETTINGS use_skip_indexes_on_data_read = 1
)
WHERE explain LIKE '%Filter column%';

SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'hello');
SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'world');
SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'hello::world');
SELECT count() FROM tab_fully WHERE hasAnyToken(text, 'hello world');

SELECT count() FROM tab_fully WHERE hasAllToken(text, 'hello');
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'world');
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'hello::world');
SELECT count() FROM tab_fully WHERE hasAllToken(text, 'hello world');

SELECT 'Partially materialized';

DROP TABLE IF EXISTS tab_partially;
CREATE TABLE tab_partially (
    id Int,
    text String
)
Engine = MergeTree()
ORDER BY id;

INSERT INTO tab_partially SELECT number, 'hello::world' from numbers(10000);

ALTER TABLE tab_partially ADD INDEX idx(text) TYPE text(tokenizer = splitByString([' ', '::']), preprocessor = lower(text));

SYSTEM STOP MERGES tab_partially;
INSERT INTO tab_partially SELECT number, 'hello world' from numbers(10000);

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'hello') SETTINGS use_skip_indexes_on_data_read = 1
)
WHERE explain LIKE '%Filter column%';

SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'hello');
SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'world');
SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'hello::world');
SELECT count() FROM tab_partially WHERE hasAnyToken(text, 'hello world');

SELECT count() FROM tab_partially WHERE hasAllToken(text, 'hello');
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'world');
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'hello::world');
SELECT count() FROM tab_partially WHERE hasAllToken(text, 'hello world');

DROP TABLE tab_partially;
DROP TABLE tab_fully;
