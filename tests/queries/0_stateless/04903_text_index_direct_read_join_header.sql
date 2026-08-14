-- Random settings limits: query_plan_direct_read_from_text_index=(1, 1); use_skip_indexes_if_final=(1, 1); use_skip_indexes=(1, 1)
-- Tags: no-random-merge-tree-settings

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_04903;

CREATE TABLE tab_04903
(
    id UInt32,
    key String,
    value String,
    INDEX idx_key (key) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = ReplacingMergeTree
ORDER BY id;

SYSTEM STOP MERGES tab_04903;

INSERT INTO tab_04903 VALUES (1, 'foo', 'foo'), (2, 'bar', 'bar'), (3, 'foo bar', 'fb');
INSERT INTO tab_04903 VALUES (1, 'foo', 'foo updated'), (2, 'baz', 'baz');

SELECT '-- PREWHERE and WHERE on the same text-indexed column, join above, FINAL';
SELECT DISTINCT t.value
FROM tab_04903 AS t FINAL
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT DISTINCT t.value
FROM tab_04903 AS t FINAL
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- without FINAL';
SELECT DISTINCT t.value
FROM tab_04903 AS t
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT DISTINCT t.value
FROM tab_04903 AS t
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- join on a key, the retained column also projected';
SELECT t.key, t.value
FROM tab_04903 AS t FINAL
GLOBAL INNER JOIN (SELECT toUInt32(number) AS rid FROM numbers(9)) AS r ON t.id = r.rid
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.key, t.value
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT t.key, t.value
FROM tab_04903 AS t FINAL
GLOBAL INNER JOIN (SELECT toUInt32(number) AS rid FROM numbers(9)) AS r ON t.id = r.rid
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.key, t.value
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- SELECT *';
SELECT *
FROM tab_04903 AS t FINAL
GLOBAL INNER JOIN (SELECT toUInt32(number) AS rid FROM numbers(9)) AS r ON t.id = r.rid
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.id, t.key, t.value, r.rid
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT *
FROM tab_04903 AS t FINAL
GLOBAL INNER JOIN (SELECT toUInt32(number) AS rid FROM numbers(9)) AS r ON t.id = r.rid
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.id, t.key, t.value, r.rid
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- join order optimizer reachable (no GLOBAL)';
SELECT DISTINCT t.value
FROM tab_04903 AS t FINAL
NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- both predicates in WHERE';
SELECT DISTINCT t.value
FROM tab_04903 AS t FINAL
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
WHERE hasTokenCaseInsensitive(t.key, 'foo') AND hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- no join';
SELECT value
FROM tab_04903 FINAL
PREWHERE hasTokenCaseInsensitive(key, 'foo')
WHERE hasAllTokens(key, 'foo')
ORDER BY value
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT '-- the direct read is still exercised by the queries above';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT DISTINCT t.value
    FROM tab_04903 AS t FINAL
    GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
    PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
    WHERE hasAllTokens(t.key, 'foo')
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_%';

SELECT '-- a Merge parent visits the same reading step twice';
DROP TABLE IF EXISTS tab_04903_second;
CREATE TABLE tab_04903_second AS tab_04903;
SYSTEM STOP MERGES tab_04903_second;
INSERT INTO tab_04903_second VALUES (7, 'foo', 'm7');

SELECT t.value
FROM merge(currentDatabase(), '^tab_04903') AS t
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT t.value
FROM merge(currentDatabase(), '^tab_04903') AS t
GLOBAL NATURAL INNER JOIN (SELECT name FROM system.columns WHERE database = 'system' AND table = 'one') AS r
PREWHERE hasTokenCaseInsensitive(t.key, 'foo')
WHERE hasAllTokens(t.key, 'foo')
ORDER BY t.value
SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE tab_04903_second;
DROP TABLE tab_04903;
