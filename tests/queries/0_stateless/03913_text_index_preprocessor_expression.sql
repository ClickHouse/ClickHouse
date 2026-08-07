-- Tests text index with preprocessor on top of an index built with an expression.

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab;

SELECT '-- Test text index with preprocessor on expression-based index';

CREATE TABLE tab
(
    key UInt64,
    s1 String,
    s2 String,
    s3 String,
    m Map(String, String),
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO tab VALUES
    (1, 'foo', ' bar', 'Hello World and more', {'Alpha': '1', 'Beta': '2'});

ALTER TABLE tab ADD INDEX idx_concat(concat(s1, s2)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(concat(s1, s2)));
ALTER TABLE tab ADD INDEX idx_valid_utf8(toValidUTF8(s3)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(toValidUTF8(s3)));
ALTER TABLE tab ADD INDEX idx_map(mapKeys(m)) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(m)));

INSERT INTO tab VALUES
    (2, 'BAZ', ' QUX', 'FOO BAR and stuff', {'GAMMA': '3', 'delta': '4'}),
    (3, 'Hello', ' World', 'test data plus extra', {'Epsilon': '5'});

SELECT '-- hasToken on concat(s1, s2)';
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'foo');
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'FOO');
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'BAZ');
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'baz');
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'Hello');
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'hello');
SELECT count() FROM tab WHERE hasToken(concat(s1, s2), 'xyz');

SELECT '-- hasAllTokens on concat(s1, s2)';
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'foo');
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'BAZ');
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'baz');
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'Hello');
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'hello');
SELECT count() FROM tab WHERE hasAllTokens(concat(s1, s2), 'xyz');

SELECT '-- hasAnyTokens on concat(s1, s2)';
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'BAZ');
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'baz');
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'Hello');
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'hello');
SELECT count() FROM tab WHERE hasAnyTokens(concat(s1, s2), 'xyz');

--

SELECT '-- hasToken on toValidUTF8(s3)';
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'Hello');
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'hello');
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'FOO');
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'foo');
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'test');
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'TEST');
SELECT count() FROM tab WHERE hasToken(toValidUTF8(s3), 'xyz');

SELECT '-- hasAllTokens on toValidUTF8(s3)';
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'Hello');
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'hello');
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'FOO');
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'foo');
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'test');
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'TEST');
SELECT count() FROM tab WHERE hasAllTokens(toValidUTF8(s3), 'xyz');

SELECT '-- hasAnyTokens on toValidUTF8(s3)';
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'Hello');
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'hello');
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'FOO');
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'test');
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'TEST');
SELECT count() FROM tab WHERE hasAnyTokens(toValidUTF8(s3), 'xyz');

--

SELECT '-- hasAllTokens on mapKeys(m)';
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'Alpha');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'ALPHA');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'gamma');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'GAMMA');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'epsilon');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'EPSILON');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'xyz');

SELECT '-- hasAnyTokens on mapKeys(m)';
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(m), 'Alpha');
SELECT count() FROM tab WHERE hasAllTokens(mapKeys(m), 'ALPHA');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(m), 'gamma');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(m), 'GAMMA');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(m), 'epsilon');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(m), 'EPSILON');
SELECT count() FROM tab WHERE hasAnyTokens(mapKeys(m), 'xyz');

DROP TABLE tab;
