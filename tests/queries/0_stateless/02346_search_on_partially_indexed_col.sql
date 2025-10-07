SET allow_experimental_full_text_index = 1;
SET parallel_replicas_local_plan = 1; -- this setting may skip index analysis when false
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByString'),
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message)
VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc baz foo'),
    (4, 'abc baz bar'),
    (5, 'abc zzz foo'),
    (6, 'abc zzz bar');

SELECT 'Establishing baseline, compare future tests to these:';
SELECT 'hasAny:';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'ba']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyToken(message, ['fo', 'ba']); -- test alias

SELECT '';
SELECT 'hasAny:';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllToken(message, ['abc', 'fo']); -- test alias

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, message)
VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar'),
    (3, 'abc baz foo');

ALTER TABLE tab ADD INDEX  idx(`message`) TYPE text(tokenizer = 'splitByString');

INSERT INTO tab(id, message)
VALUES
    (4, 'abc baz bar'),
    (5, 'abc zzz foo'),
    (6, 'abc zzz bar');

SELECT '';
SELECT 'Should match baseline:';
SELECT 'hasAny:';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'ba']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyToken(message, ['fo', 'ba']); -- test alias

SELECT '';
SELECT 'hasAny:';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllToken(message, ['abc', 'fo']); -- test alias

DROP TABLE tab;
