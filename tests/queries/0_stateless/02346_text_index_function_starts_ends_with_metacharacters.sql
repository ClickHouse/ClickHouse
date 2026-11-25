-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN checks read the plan of the initiator query.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/118197
-- A text index with the `array` tokenizer matches a `startsWith`/`endsWith` needle literally, also when it contains `%`, `_` or `\`.
-- Each query is paired with the same query with `use_skip_indexes = 0`: a skip index must never change the result.

SET explain_query_plan_default = 'legacy';
-- The text index answers these queries alone, without re-checking the predicate.
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
-- Rewrites `LIKE 'foobar\%%'` into `startsWith(tag, 'foobar%')`.
SET optimize_rewrite_like_perfect_affix = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'foobar%tail'), (2, 'foobarXtail'), (3, 'foobar'), (4, 'zzfoobar'), (5, 'zz%foobar'), (6, '%foobarZ'), (7, 'xxfoobarxx'),
    (8, 'xxfoobar%'), (9, 'foo_bar1'), (10, 'fooXbar1'), (11, 'foobar\\tail'), (12, 'foobar%'), (13, 'xxfoobar\\');

SELECT 'startsWith, % at the end', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, 'foobar%');
SELECT 'startsWith, % at the end', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, 'foobar%') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith, % at the start', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, '%foobar');
SELECT 'endsWith, % at the start', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, '%foobar') SETTINGS use_skip_indexes = 0;
SELECT 'LIKE with an escaped %', arraySort(groupArray(id)) FROM tab WHERE tag LIKE 'foobar\%%';
SELECT 'LIKE with an escaped %', arraySort(groupArray(id)) FROM tab WHERE tag LIKE 'foobar\%%' SETTINGS use_skip_indexes = 0;
SELECT 'startsWith, % at the start', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, '%foobar');
SELECT 'startsWith, % at the start', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, '%foobar') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith, % at the end', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, 'foobar%');
SELECT 'endsWith, % at the end', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, 'foobar%') SETTINGS use_skip_indexes = 0;
SELECT 'startsWith, _', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, 'foo_bar');
SELECT 'startsWith, _', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, 'foo_bar') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith, _', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, 'foo_bar1');
SELECT 'endsWith, _', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, 'foo_bar1') SETTINGS use_skip_indexes = 0;
SELECT 'startsWith, backslash at the end', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, 'foobar\\');
SELECT 'startsWith, backslash at the end', arraySort(groupArray(id)) FROM tab WHERE startsWith(tag, 'foobar\\') SETTINGS use_skip_indexes = 0;
SELECT 'endsWith, backslash at the end', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, 'foobar\\');
SELECT 'endsWith, backslash at the end', arraySort(groupArray(id)) FROM tab WHERE endsWith(tag, 'foobar\\') SETTINGS use_skip_indexes = 0;

-- Columns: the plan reads the text index, the plan still evaluates the original function.
SELECT 'startsWith, % at the end', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE startsWith(tag, 'foobar%'));
SELECT 'endsWith, % at the start', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE endsWith(tag, '%foobar'));
SELECT 'LIKE with an escaped %', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE tag LIKE 'foobar\%%');
SELECT 'startsWith, % at the start', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE startsWith(tag, '%foobar'));
SELECT 'endsWith, % at the end', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE endsWith(tag, 'foobar%'));
SELECT 'startsWith, _', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE startsWith(tag, 'foo_bar'));
SELECT 'endsWith, _', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE endsWith(tag, 'foo_bar1'));
SELECT 'startsWith, backslash at the end', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE startsWith(tag, 'foobar\\'));
SELECT 'endsWith, backslash at the end', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(match(explain, 'FUNCTION (startsWith|endsWith|like)\\(')) > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE endsWith(tag, 'foobar\\'));

DROP TABLE tab;
