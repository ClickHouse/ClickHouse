SET allow_experimental_full_text_index = 1;

-- Tests text index with the 'CoalescingMergeTree' engine

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    key String,
    value Nullable(String),
    INDEX idx_key(key) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = CoalescingMergeTree()
ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES
    (1, 'foo', 'foo'),
    (2, 'bar', NULL);

INSERT INTO tab VALUES
    (1, 'foo', NULL),
    (2, 'bar', 'bar');

SELECT 'Take value from the first part';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab WHERE hasToken(key, 'foo') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'foo') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab WHERE hasToken(key, 'foo') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'foo') ORDER BY value;

SELECT 'Take value from the second part';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab WHERE hasToken(key, 'bar') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'bar') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab WHERE hasToken(key, 'bar') ORDER BY value;
SELECT value FROM tab FINAL WHERE hasToken(key, 'bar') ORDER BY value;

DROP TABLE tab;
