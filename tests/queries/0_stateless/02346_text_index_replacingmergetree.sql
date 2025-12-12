SET allow_experimental_full_text_index = 1;

-- Tests text index with the 'ReplacingMergeTree' engine

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    key String,
    value String,
    INDEX idx_key(key) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = ReplacingMergeTree()
ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES
    (1, 'foo', 'foo'),
    (2, 'bar', 'bar');

INSERT INTO tab VALUES
    (1, 'foo', 'foo updated'),
    (2, 'baz', 'baz');

SELECT 'Updated: foo';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab WHERE hasToken(key, 'foo') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'foo') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab WHERE hasToken(key, 'foo') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'foo') ORDER BY value;

SELECT 'Removed: bar';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT '-- -- value exists without FINAL';
SELECT value FROM tab WHERE hasToken(key, 'bar') ORDER BY value;

SELECT '-- -- value does not exist with FINAL';
SELECT value FROM tab FINAL WHERE hasToken(key, 'bar') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT '-- -- value exists without FINAL';
SELECT value FROM tab WHERE hasToken(key, 'bar') ORDER BY value;

SELECT '-- -- value does not exist with FINAL';
SELECT value FROM tab FINAL WHERE hasToken(key, 'bar') ORDER BY value;

SELECT 'New: baz';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab WHERE hasToken(key, 'baz') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'baz') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab WHERE hasToken(key, 'baz') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'baz') ORDER BY value;

DROP TABLE tab;
