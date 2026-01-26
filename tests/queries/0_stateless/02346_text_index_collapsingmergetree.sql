SET allow_experimental_full_text_index = 1;

-- Tests text index with the 'CollapsingMergeTree' engine

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    key String,
    value Nullable(String),
    sign Int8,
    INDEX idx_key(key) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'foo', 'foo', -1),
    (2, 'bar', 'bar', -1);

INSERT INTO tab VALUES
    (1, 'foo', 'foo updated', 1),
    (2, 'bar', 'bar updated', 1);

SELECT 'Take values from all parts';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab WHERE hasToken(key, 'foo') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab WHERE hasToken(key, 'bar') ORDER BY value;

SELECT 'Take values from the final part';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab FINAL WHERE hasToken(key, 'foo') ORDER BY value;

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab FINAL WHERE hasToken(key, 'bar') ORDER BY value;

DROP TABLE tab;
