-- Regression test: a text index whose `preprocessor` argument references an ALIAS
-- column used to fail validation with "Missing columns: '<alias>'". The ALIAS-to-
-- expression substitution in IndexDescription::getIndexFromAST only rewrote the index
-- KEY expression, leaving the TYPE argument VALUES untouched, so at argument-validation
-- time only the physical source columns existed and `lower(alias_col)` could not resolve.
-- The fix applies the same alias substitution to the text index argument values.

SET enable_full_text_index = 1;

SELECT '-- CREATE: preprocessor over an ALIAS column';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    title String,
    body String,
    combined ALIAS concat(title, ' ', body),
    INDEX idx combined TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(combined)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('Hello', 'World'), ('FOO', 'BAR'), ('quux', 'zoo');

-- The original DDL (with the ALIAS name) must be preserved in the argument: alias substitution
-- happens on the working argument copy, not on the definition AST used for serialization. The
-- resolved index KEY expression is the expanded ALIAS `concat(title, ' ', body)`.
SELECT '-- index metadata keeps the ALIAS name in the argument, key is the expanded expression';
SELECT type_full, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab' AND name = 'idx';

-- Tokens are lowercased at index time by the preprocessor; the needle is preprocessed too.
SELECT '-- query results';
SELECT count() FROM tab WHERE hasToken(combined, 'hello');
SELECT count() FROM tab WHERE hasToken(combined, 'Hello');
SELECT count() FROM tab WHERE hasToken(combined, 'world');
SELECT count() FROM tab WHERE hasToken(combined, 'foo');
SELECT count() FROM tab WHERE hasToken(combined, 'FOO');
SELECT count() FROM tab WHERE hasToken(combined, 'bar');
SELECT count() FROM tab WHERE hasToken(combined, 'quux');
SELECT count() FROM tab WHERE hasToken(combined, 'xyz');

-- The index is actually built and used for the lookup.
SELECT '-- index is used';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM tab WHERE hasToken(combined, 'hello')) WHERE explain LIKE '%Name: idx%';

DROP TABLE tab;

SELECT '-- DETACH / ATTACH round-trip (recalculateWithNewColumns)';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    title String,
    body String,
    combined ALIAS concat(title, ' ', body),
    INDEX idx combined TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(combined)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('Hello', 'World');
DETACH TABLE tab;
ATTACH TABLE tab;
SELECT count() FROM tab WHERE hasToken(combined, 'hello');
DROP TABLE tab;

SELECT '-- ALTER ADD INDEX / MATERIALIZE INDEX';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    title String,
    body String,
    combined ALIAS concat(title, ' ', body)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('Hello', 'World'), ('FOO', 'BAR');
ALTER TABLE tab ADD INDEX idx combined TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(combined)) GRANULARITY 1;
ALTER TABLE tab MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT count() FROM tab WHERE hasToken(combined, 'hello');
SELECT count() FROM tab WHERE hasToken(combined, 'foo');
DROP TABLE tab;

-- Guard: an ALIAS column named exactly like a parameter key (`tokenizer`) must not clobber
-- the key. Only the value side of each key=value pair is alias-substituted, so the argument
-- key `tokenizer` keeps its meaning and parsing still succeeds.
SELECT '-- ALIAS column shadowing a parameter name does not break argument parsing';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    title String,
    body String,
    combined ALIAS concat(title, ' ', body),
    tokenizer ALIAS 'shadow',
    INDEX idx combined TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(combined)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('Hello', 'World'), ('FOO', 'BAR');
SELECT count() FROM tab WHERE hasToken(combined, 'hello');
SELECT count() FROM tab WHERE hasToken(combined, 'foo');
DROP TABLE tab;
