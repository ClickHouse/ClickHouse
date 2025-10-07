SET allow_experimental_full_text_index = 1;
SET parallel_replicas_local_plan = 1; -- this setting may skip index analysis when false
SET use_skip_indexes_on_data_read = 0;
SET mutations_sync = 2; -- want synchronous materialize

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(`message`) TYPE text(tokenizer = 'splitByString') GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY (id) SETTINGS index_granularity = 2;

DROP VIEW IF EXISTS explain_indexes;
CREATE VIEW explain_indexes
  AS  SELECT *
    FROM viewExplain('EXPLAIN', 'indexes = 1', (
        SELECT groupArray(id) FROM tab WHERE hasAnyTokens(message, ['def'])
    ));

-- bar# tests that same tokenizer is used for brute force search
INSERT INTO tab(id, message)
VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar#'),
    (3, 'abc baz foo'),
    (4, 'abc baz bar'),
    (5, 'abc zzz foo'),
    (6, 'abc zzz bar');

-- { echoOn }
SELECT * FROM explain_indexes;
--
--Establishing baseline, compare future tests to these:
--hasAnyTokens:
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'ba']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['fo', 'ba']);
--
--hasAllTokens:
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'fo']);
-- { echoOff }

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
)
ENGINE = MergeTree
ORDER BY (id) SETTINGS index_granularity = 2;

SYSTEM STOP MERGES tab;

-- bar# tests that same tokenizer is used for brute force search
INSERT INTO tab(id, message)
VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar#'),
    (3, 'abc baz foo');

ALTER TABLE tab ADD INDEX  idx(`message`) TYPE text(tokenizer = 'splitByString') GRANULARITY 1;

INSERT INTO tab(id, message)
VALUES
    (4, 'abc baz bar'),
    (5, 'abc zzz foo'),
    (6, 'abc zzz bar');

-- { echoOn }
SELECT * FROM explain_indexes;

--
--Should match baseline:
--hasAnyTokens:
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['foo', 'ba']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['fo', 'ba']);

--
--hasAllTokens:
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['ab']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'foo']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['foo', 'bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['abc', 'fo']);
-- { echoOff }

DROP TABLE tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
)
ENGINE = MergeTree
ORDER BY (id) SETTINGS index_granularity = 2;

INSERT INTO tab(id, message)
VALUES
    (1, 'abc def foo'),
    (2, 'abc def bar#'),
    (3, 'abc baz foo');

ALTER TABLE tab ADD INDEX  idx(`message`) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;

INSERT INTO tab(id, message)
VALUES
    (4, 'abc baz bar'),
    (5, 'abc zzz foo'),
    (6, 'abc zzz bar');

-- { echoOn }
SELECT * FROM explain_indexes;

--
-- Test that splitByNonAlpha tokenizer is applied even to column part which is not materialized
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAnyTokens(message, ['bar']);
SELECT arraySort(groupArray(id)) FROM tab WHERE hasAllTokens(message, ['bar']);
-- { echoOff }

DROP TABLE tab;
DROP VIEW explain_indexes;
SYSTEM START MERGES tab;

