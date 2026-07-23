-- Test text index preprocessor validation with various string-like column types.
-- Before the fix, the preprocessor required exact type match between output and
-- source column type. The fix relaxes this to check only that the base type
-- (after unwrapping Array/Nullable/LowCardinality) is String or FixedString.

DROP TABLE IF EXISTS tab;

-- The main use case: CAST(val, 'String') as preprocessor normalizes any
-- string-like column to plain String before tokenization.

SELECT 'CAST(val, String) on non-Nullable scalar types';

SELECT '-- String';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- FixedString';
CREATE TABLE tab (id UInt64, val FixedString(3), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- LowCardinality(String)';
CREATE TABLE tab (id UInt64, val LowCardinality(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- LowCardinality(FixedString)';
CREATE TABLE tab (id UInt64, val LowCardinality(FixedString(3)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT 'CAST(val, String) on Array types';

SELECT '-- Array(String)';
CREATE TABLE tab (id UInt64, val Array(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['foo']), (2, ['bar']), (3, ['baz']);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Array(FixedString)';
CREATE TABLE tab (id UInt64, val Array(FixedString(3)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['foo']), (2, ['bar']), (3, ['baz']);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Array(LowCardinality(String))';
CREATE TABLE tab (id UInt64, val Array(LowCardinality(String)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['foo']), (2, ['bar']), (3, ['baz']);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

-- For Nullable types, CAST(val, 'String') would fail at runtime when NULLs are present.
-- Use type-appropriate CASTs that preserve the Nullable wrapper.

SELECT 'CAST on Nullable types';

SELECT '-- Nullable(String) + CAST to String (non-NULL data)';
CREATE TABLE tab (id UInt64, val Nullable(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Nullable(FixedString) + CAST to Nullable(String)';
CREATE TABLE tab (id UInt64, val Nullable(FixedString(3)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'Nullable(String)'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, NULL), (3, 'bar');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- LowCardinality(Nullable(String)) + CAST to String (non-NULL data)';
CREATE TABLE tab (id UInt64, val LowCardinality(Nullable(String)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'String'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Array(Nullable(String)) + CAST to Nullable(String)';
CREATE TABLE tab (id UInt64, val Array(Nullable(String)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'Nullable(String)'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['foo']), (2, [NULL, 'bar']), (3, [NULL]);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

SELECT 'Other CAST directions';

SELECT '-- String + CAST to FixedString';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'FixedString(3)'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- String + CAST to LowCardinality(String)';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'LowCardinality(String)'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- String + CAST to Nullable(String)';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'Nullable(String)'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- String + CAST to LowCardinality(Nullable(FixedString))';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'LowCardinality(Nullable(FixedString(3)))'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- LowCardinality(String) + CAST to Nullable(FixedString)';
CREATE TABLE tab (id UInt64, val LowCardinality(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = CAST(val, 'Nullable(FixedString(3))'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT 'Type-preserving preprocessor (lower)';

SELECT '-- Nullable(String) + lower';
CREATE TABLE tab (id UInt64, val Nullable(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'FOO'), (2, NULL), (3, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- LowCardinality(String) + lower';
CREATE TABLE tab (id UInt64, val LowCardinality(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'FOO'), (2, 'BAR'), (3, 'BAZ');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- LowCardinality(Nullable(String)) + lower';
CREATE TABLE tab (id UInt64, val LowCardinality(Nullable(String)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, 'FOO'), (2, NULL), (3, 'BAR');
SELECT count() FROM tab WHERE hasAllTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAllTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Array(String) + lower';
CREATE TABLE tab (id UInt64, val Array(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['FOO']), (2, ['BAR']), (3, ['BAZ']);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Array(Nullable(String)) + lower';
CREATE TABLE tab (id UInt64, val Array(Nullable(String)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['FOO']), (2, [NULL, 'BAR']), (3, [NULL]);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

SELECT '-- Array(LowCardinality(String)) + lower';
CREATE TABLE tab (id UInt64, val Array(LowCardinality(String)), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(val))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['FOO']), (2, ['BAR']), (3, ['BAZ']);
SELECT count() FROM tab WHERE hasAnyTokens(val, 'foo');
SELECT count() FROM tab WHERE hasAnyTokens(val, 'xyz');
DROP TABLE tab;

SELECT 'Negative tests';

SELECT '-- Preprocessor returning UInt64 should fail';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = length(val))) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- Preprocessor returning Array(UInt64) should fail';
CREATE TABLE tab (id UInt64, val Array(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = length(val))) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- Preprocessor wrapping scalar String in Array should fail';
CREATE TABLE tab (id UInt64, val String, INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = array(val))) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- Preprocessor wrapping scalar LowCardinality(String) in Array should fail';
CREATE TABLE tab (id UInt64, val LowCardinality(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = array(val))) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT '-- Preprocessor adding extra array dimension to Array(String) should fail';
CREATE TABLE tab (id UInt64, val Array(String), INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = array(val))) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_QUERY }
