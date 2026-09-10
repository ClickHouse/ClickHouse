-- Tests the documented pattern for excluding overly long tokens: a length-based postprocessor
-- with the splitByNonAlpha tokenizer, which keeps punctuation-separated words searchable.

DROP TABLE IF EXISTS tab;

SELECT '1. Length postprocessor with splitByNonAlpha.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(tokenizer = 'splitByNonAlpha', postprocessor = if(length(val) > 32, '', val))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'error:failed e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'),
    (2, 'foo,bar short'),
    (3, 'plain text');

-- Punctuation-separated words are still indexed as individual tokens.
SELECT count() FROM tab WHERE hasToken(val, 'error');
SELECT count() FROM tab WHERE hasToken(val, 'failed');
SELECT count() FROM tab WHERE hasToken(val, 'foo');
SELECT count() FROM tab WHERE hasToken(val, 'bar');
SELECT count() FROM tab WHERE hasToken(val, 'short');
SELECT count() FROM tab WHERE hasToken(val, 'plain');

-- The 64-character hash token maps to '' and is never indexed, so it is not found.
SELECT count() FROM tab WHERE hasToken(val, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');

DROP TABLE tab;

SELECT '2. Preprocessor strips UUIDs that splitByNonAlpha would split into short tokens.';

CREATE TABLE tab
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = replaceRegexpAll(val, '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', ' '),
        postprocessor = if(length(val) > 32, '', val)
    )
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES
    (1, 'request 550e8400-e29b-41d4-a716-446655440000 failed'),
    (2, 'request ok');

-- The UUID is removed before tokenization, so none of its parts are indexed.
SELECT count() FROM tab WHERE hasToken(val, '550e8400');
SELECT count() FROM tab WHERE hasToken(val, '446655440000');
-- The remaining words are tokenized by splitByNonAlpha as usual.
SELECT count() FROM tab WHERE hasToken(val, 'request');
SELECT count() FROM tab WHERE hasToken(val, 'failed');

DROP TABLE tab;
