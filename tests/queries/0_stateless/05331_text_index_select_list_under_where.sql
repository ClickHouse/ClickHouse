-- A text-search function outside the filter follows the text index tokenizer, preprocessor and postprocessor
-- whether or not the query has a WHERE clause.

DROP TABLE IF EXISTS tq;
DROP TABLE IF EXISTS tp;
DROP TABLE IF EXISTS tg;
DROP TABLE IF EXISTS te;
DROP TABLE IF EXISTS tm;
DROP TABLE IF EXISTS ta;

CREATE TABLE tq (id UInt32, s String, s2 String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tq VALUES (1, 'xyz', 'xyz'), (2, 'Hello World', 'Hello World'), (3, 'foo', 'foo');

CREATE TABLE tp (id UInt32, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tp VALUES (1, 'xyz'), (2, 'Hello World'), (3, 'foo');

CREATE TABLE tg (id UInt32, s String, INDEX idx s TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tg VALUES (1, 'xyz'), (2, 'hello world'), (3, 'foo');

CREATE TABLE te (id UInt32, s String, INDEX idx lower(s) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO te VALUES (1, 'xyz'), (2, 'Hello World'), (3, 'foo');

CREATE TABLE tm (id UInt32, m Map(String, String), INDEX idx mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tm VALUES (1, {'k': 'xyz'}), (2, {'k': 'hello world'}), (3, {'k': 'foo'});

CREATE TABLE ta (id UInt32, a Array(String), INDEX idx a TYPE text(tokenizer = array, postprocessor = lower(a)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO ta VALUES (1, ['xyz']), (2, ['Hello']), (3, ['foo']);

SELECT 'tq', countIf(hasToken(s, 'hello')), countIf(hasAnyTokens(s, 'hello')), countIf(hasAllTokens(s, 'hello world')), countIf(hasPhrase(s, 'hello world')) FROM tq;
SELECT 'tq where', countIf(hasToken(s, 'hello')), countIf(hasAnyTokens(s, 'hello')), countIf(hasAllTokens(s, 'hello world')), countIf(hasPhrase(s, 'hello world')) FROM tq WHERE id > 0 SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT 'tq where, no prewhere', countIf(hasAnyTokens(s, 'hello')) FROM tq WHERE id > 0 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT 'tq where, subquery', countIf(hasAnyTokens(x, 'hello')) FROM (SELECT id, s AS x FROM tq WHERE id > 0);
SELECT 'tq where, order by s', id, hasAnyTokens(s, 'hello') FROM tq WHERE id > 0 ORDER BY s;
SELECT 'tq unmerged expressions', countIf(hasAnyTokens(s, 'hello')) FROM tq SETTINGS query_plan_merge_expressions = 0;
-- `s` is a renamed column without an index, so the default tokenizer applies.
SELECT 'tq where, renamed column', countIf(hasAnyTokens(s, 'hello')) FROM (SELECT id, s2 AS s FROM tq WHERE id > 0);
SELECT 'tq where, order by id', id, hasAnyTokens(s, 'hello') FROM tq WHERE id > 0 ORDER BY id;
SELECT 'tq where, tuple', arraySort(groupArray((id, hasAnyTokens(s, 'hello')))) FROM tq WHERE id > 0;

SELECT 'tp', countIf(hasAnyTokens(s, 'HELLO')) FROM tp;
SELECT 'tp where, order by id', id, hasAnyTokens(s, 'HELLO') FROM tp WHERE id > 0 ORDER BY id;
SELECT 'tp where, unmerged expressions', countIf(hasAnyTokens(s, 'HELLO')) FROM tp WHERE id > 0 SETTINGS query_plan_merge_expressions = 0;

SELECT 'tg', countIf(hasAnyTokens(s, 'hel')) FROM tg;
SELECT 'tg where', countIf(hasAnyTokens(s, 'hel')) FROM tg WHERE id > 0;

SELECT 'te', countIf(hasAnyTokens(lower(s), 'hel')) FROM te;
SELECT 'te where', countIf(hasAnyTokens(lower(s), 'hel')) FROM te WHERE id > 0;

SELECT 'tm', countIf(hasAnyTokens(m['k'], 'hel')) FROM tm;
SELECT 'tm where', countIf(hasAnyTokens(m['k'], 'hel')) FROM tm WHERE id > 0;

SELECT 'ta', countIf(hasAnyTokens(a, ['hello'])) FROM ta;
SELECT 'ta where', countIf(hasAnyTokens(a, ['hello'])) FROM ta WHERE id > 0;

DROP TABLE tq;
DROP TABLE tp;
DROP TABLE tg;
DROP TABLE te;
DROP TABLE tm;
DROP TABLE ta;
