-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102497
-- hasToken with separator-only needles should throw BAD_ARGUMENTS consistently,
-- regardless of whether a text index is present.
-- hasTokenOrNull with separator-only needles should return NULL per row,
-- not silently skip all granules.

SELECT 'Scalar baseline.';

SELECT '- hasTokenOrNull with separator-only needle returns NULL';
SELECT hasTokenOrNull('hello', '()');

SELECT 'Without text index.';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int32, text String) ENGINE = MergeTree() ORDER BY (id);
INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar');

SELECT '- hasToken with separator-only needle throws BAD_ARGUMENTS';
SELECT count() FROM tab WHERE hasToken(text, '()');     -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE NOT hasToken(text, '()'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE hasToken(text, '!!!');    -- { serverError BAD_ARGUMENTS }

SELECT '- hasTokenOrNull with separator-only needle returns NULL per row';
SELECT count() FROM tab WHERE isNull(hasTokenOrNull(text, '()'));

SELECT '- Empty needle matches nothing, no exception';
SELECT count() FROM tab WHERE hasToken(text, '');
SELECT count() FROM tab WHERE NOT hasToken(text, '');

DROP TABLE tab;

SELECT 'With text index.';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id Int32,
    text String,
    INDEX idx_text(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES (1, 'hello world'), (2, 'foo bar');

SELECT '- hasToken with separator-only needle throws BAD_ARGUMENTS';
SELECT count() FROM tab WHERE hasToken(text, '()');     -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE NOT hasToken(text, '()'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tab WHERE hasToken(text, '!!!');    -- { serverError BAD_ARGUMENTS }

SELECT '- hasTokenOrNull with separator-only needle returns NULL per row';
SELECT count() FROM tab WHERE isNull(hasTokenOrNull(text, '()'));

SELECT '- Empty needle matches nothing, no exception';
SELECT count() FROM tab WHERE hasToken(text, '');
SELECT count() FROM tab WHERE NOT hasToken(text, '');

DROP TABLE tab;
