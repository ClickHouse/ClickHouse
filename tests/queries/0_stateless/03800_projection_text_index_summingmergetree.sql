SET enable_full_text_index = 1;

-- Tests text index with the 'SummingMergeTree' engine

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    key String,
    value UInt32,
    PROJECTION idx_key INDEX key TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = SummingMergeTree()
ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES
    (1, 'foo', 1),
    (2, 'bar', 2);

INSERT INTO tab VALUES
    (1, 'foo', 1),
    (2, 'bar', 2);

SELECT 'Sum values from all parts';

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT sum(value) FROM tab WHERE hasToken(key, 'foo');
SELECT sum(value) FROM tab WHERE hasToken(key, 'bar');

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT sum(value) FROM tab WHERE hasToken(key, 'foo');
SELECT sum(value) FROM tab WHERE hasToken(key, 'bar');


SELECT 'Values are summed up during merge';

SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL; -- emulate merge 

SELECT '-- direct read disabled';

SET use_skip_indexes_on_data_read = 0;

SELECT value FROM tab WHERE hasToken(key, 'foo');
SELECT value FROM tab WHERE hasToken(key, 'bar');

SELECT '-- direct read enabled';

SET use_skip_indexes_on_data_read = 1;

SELECT value FROM tab WHERE hasToken(key, 'foo');
SELECT value FROM tab WHERE hasToken(key, 'bar');

DROP TABLE tab;
