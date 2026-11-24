-- Tags: no-parallel-replicas
-- Tests that affix LIKE/ILIKE patterns, i.e. prefix ('value%') and suffix ('%value'), use the text index as a hint.
-- By default the analyzer rewrites such patterns into startsWith/endsWith (optimize_rewrite_like_perfect_affix),
-- so both spellings are covered here.
SET explain_query_plan_default = 'legacy';

SET enable_analyzer = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
-- Pinned because the queries below assert which of the two spellings the plan ends up with.
SET optimize_rewrite_like_perfect_affix = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY (id);

-- The dictionary of this table contains tokens that match the prefix/suffix patterns below in rows
-- where the whole value does not match them, so a hint that is not verified would return extra rows.
INSERT INTO tab(id, message) VALUES
    (1, 'foobar baz'),
    (2, 'baz foobar'),
    (3, 'foobarqux end'),
    (4, 'end foobarqux'),
    (5, 'quuxfoobar'),
    (6, 'quuxfoobar tail'),
    (7, 'nothing here');

SELECT 'Test results are same with/without the optimization';

SELECT '-- without optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE 'foobar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE 'nonexistent%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%nonexistent';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' AND message LIKE '%baz';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' OR message LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' AND hasToken(message, 'baz');
SELECT groupArray(id) FROM tab WHERE startsWith(message, 'foobar');
SELECT groupArray(id) FROM tab WHERE endsWith(message, 'foobar');
SELECT groupArray(id) FROM tab WHERE startsWith(message, 'foobar baz');
SELECT groupArray(id) FROM tab WHERE endsWith(message, 'quuxfoobar tail');
SELECT groupArray(id) FROM tab WHERE message ILIKE 'FOOBAR%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOOBAR';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE 'foobar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE 'nonexistent%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%nonexistent';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' AND message LIKE '%baz';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' OR message LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' AND hasToken(message, 'baz');
SELECT groupArray(id) FROM tab WHERE startsWith(message, 'foobar');
SELECT groupArray(id) FROM tab WHERE endsWith(message, 'foobar');
SELECT groupArray(id) FROM tab WHERE startsWith(message, 'foobar baz');
SELECT groupArray(id) FROM tab WHERE endsWith(message, 'quuxfoobar tail');
SELECT groupArray(id) FROM tab WHERE message ILIKE 'FOOBAR%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOOBAR';
SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%' SETTINGS optimize_rewrite_like_perfect_affix = 0;
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar' SETTINGS optimize_rewrite_like_perfect_affix = 0;

SELECT '-- with optimization but without hints';

SET query_plan_text_index_add_hint = 0;

SELECT groupArray(id) FROM tab WHERE message LIKE 'foobar%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message LIKE '%foobar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE 'foobar%';
SELECT groupArray(id) FROM tab WHERE message NOT LIKE '%foobar';
SELECT groupArray(id) FROM tab WHERE message ILIKE 'FOOBAR%';
SELECT groupArray(id) FROM tab WHERE message ILIKE '%FOOBAR';

SET query_plan_text_index_add_hint = 1;

SELECT 'Prefix and suffix patterns keep the original condition, infix patterns do not';

-- The columns are: whether the query plan contains a text index virtual column, and whether it still
-- evaluates the original search function. The latter is absent only for an exact direct read.
SELECT 'prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION startsWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message LIKE 'foobar%');

SELECT 'suffix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION endsWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message LIKE '%foobar');

SELECT 'prefix, no rewrite', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message LIKE 'foobar%' SETTINGS optimize_rewrite_like_perfect_affix = 0);

SELECT 'suffix, no rewrite', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message LIKE '%foobar' SETTINGS optimize_rewrite_like_perfect_affix = 0);

SELECT 'infix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION like(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message LIKE '%foobar%');

SELECT 'ilike prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION ilike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message ILIKE 'FOOBAR%');

SELECT 'Needles shorter than text_index_like_min_pattern_length are not evaluated by a dictionary scan';

SELECT 'prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION startsWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE message LIKE 'foo%');

SELECT groupArray(id) FROM tab WHERE message LIKE 'foo%';
SELECT groupArray(id) FROM tab WHERE message LIKE '%baz';

DROP TABLE tab;

SELECT 'Text index analysis';

CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (id)
SETTINGS index_granularity = 1;

INSERT INTO tab SELECT number, 'Hello ClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hello World, ClickHouse is fast!' FROM numbers(1024);
INSERT INTO tab SELECT number, 'Hallo xClickHouse' FROM numbers(1024);
INSERT INTO tab SELECT number, 'ClickHousez rocks' FROM numbers(1024);

SELECT '-- Prefix pattern should choose 1 part and 1024 granules out of 4 parts and 4096 granules';
-- Only 'ClickHousez rocks' has a token starting with 'ClickHousez'.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE 'ClickHousez%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Suffix pattern should choose 3 parts and 3072 granules out of 4 parts and 4096 granules';
-- 'ClickHouse' and 'xClickHouse' are tokens ending with 'ClickHouse', only 'ClickHousez' is not.
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE '%ClickHouse'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

SELECT '-- Prefix pattern with a non-existent token should choose none';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes=1
    SELECT count() FROM tab WHERE message LIKE 'random%'
) WHERE explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%'
LIMIT 2, 3;

-- Three parts have a token starting with 'ClickHouse' but only one of them has rows starting with it,
-- so the hint must be verified by the original condition.
SELECT count() FROM tab WHERE message LIKE 'ClickHouse%';
SELECT count() FROM tab WHERE message LIKE 'ClickHouse%' SETTINGS use_skip_indexes = 0;

SELECT count() FROM tab WHERE message LIKE '%ClickHouse';
SELECT count() FROM tab WHERE message LIKE '%ClickHouse' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'Test results are same with/without the optimization with array tokenizer';

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, tag) VALUES
    (1, 'ClickHouseServer'),
    (2, 'clickhouseClient'),
    (3, 'ClickHouseCloud'),
    (4, 'CLICKHOUSE_SQL');

SELECT '-- without optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 0;

SELECT groupArray(id) FROM tab WHERE tag LIKE 'ClickHouse%';
SELECT groupArray(id) FROM tab WHERE tag LIKE '%Cloud';
SELECT groupArray(id) FROM tab WHERE tag NOT LIKE 'ClickHouse%';
SELECT groupArray(id) FROM tab WHERE startsWith(tag, 'ClickHouse');
SELECT groupArray(id) FROM tab WHERE endsWith(tag, 'Cloud');
SELECT groupArray(id) FROM tab WHERE tag ILIKE 'clickhouse%';
SELECT groupArray(id) FROM tab WHERE tag ILIKE '%cloud';

SELECT '-- with optimization';

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

SELECT groupArray(id) FROM tab WHERE tag LIKE 'ClickHouse%';
SELECT groupArray(id) FROM tab WHERE tag LIKE '%Cloud';
SELECT groupArray(id) FROM tab WHERE tag NOT LIKE 'ClickHouse%';
SELECT groupArray(id) FROM tab WHERE startsWith(tag, 'ClickHouse');
SELECT groupArray(id) FROM tab WHERE endsWith(tag, 'Cloud');
SELECT groupArray(id) FROM tab WHERE tag ILIKE 'clickhouse%';
SELECT groupArray(id) FROM tab WHERE tag ILIKE '%cloud';

DROP TABLE tab;

SELECT 'With the array tokenizer a token is the whole value, so an affix is exact';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    tag String,
    INDEX idx(tag) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, tag) VALUES
    (1, 'ClickHouseServer'),
    (2, 'clickhouseClient'),
    (3, 'ClickHouseCloud'),
    (4, 'CLICKHOUSE_SQL');

SELECT 'prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION startsWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE tag LIKE 'ClickHouse%');

SELECT 'suffix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION endsWith(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE tag LIKE '%Cloud');

SELECT 'ilike prefix', countIf(explain LIKE '%\_\_text_index\_%') > 0, countIf(explain LIKE '%FUNCTION ilike(%') > 0
FROM (EXPLAIN actions = 1 SELECT count() FROM tab WHERE tag ILIKE 'clickhouse%');

DROP TABLE tab;

SELECT 'A nullable value is left to the original condition';

CREATE TABLE tab
(
    id UInt32,
    tag Nullable(String),
    INDEX idx(tag) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY (id);

INSERT INTO tab(id, tag) VALUES
    (1, 'ClickHouseServer'),
    (2, 'clickhouseClient'),
    (3, 'ClickHouseCloud'),
    (4, NULL);

SELECT groupArray(id) FROM tab WHERE tag LIKE 'ClickHouse%';
SELECT groupArray(id) FROM tab WHERE tag LIKE 'ClickHouse%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE tag NOT LIKE 'ClickHouse%';
SELECT groupArray(id) FROM tab WHERE tag NOT LIKE 'ClickHouse%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE NOT endsWith(tag, 'Cloud');
SELECT groupArray(id) FROM tab WHERE NOT endsWith(tag, 'Cloud') SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE tag NOT ILIKE 'clickhouse%';
SELECT groupArray(id) FROM tab WHERE tag NOT ILIKE 'clickhouse%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE tag NOT LIKE '%ClickHouse%';
SELECT groupArray(id) FROM tab WHERE tag NOT LIKE '%ClickHouse%' SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM tab WHERE tag NOT ILIKE '%clickhouse%';
SELECT groupArray(id) FROM tab WHERE tag NOT ILIKE '%clickhouse%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT 'An affix hint that prunes nothing is discarded, granule pruning is kept';

CREATE TABLE tab
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, multiIf(number < 1000, 'clickhouse is fast', number < 30000, 'bank of the river', 'alpha beta gamma') FROM numbers(100000);

SELECT count() FROM tab WHERE message LIKE 'clickhouse%' SETTINGS log_comment = 'affix_hint_selective';
SELECT count() FROM tab WHERE message LIKE 'river%' SETTINGS log_comment = 'affix_hint_nonselective';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       max(ProfileEvents['TextIndexUseHint'] > 0) AS hint_used,
       max(ProfileEvents['TextIndexDiscardHint'] > 0) AS hint_discarded,
       max(read_rows < 100000) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish' AND log_comment IN ('affix_hint_selective', 'affix_hint_nonselective')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE tab;

SELECT 'A nullable value reached through mapValues is left to the original condition';

CREATE TABLE tab
(
    id UInt64,
    m Map(String, Nullable(String)),
    INDEX idx mapValues(m) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT number, map('k', if(number = 0, 'foobar value', 'zulu yankee')) FROM numbers(500);
INSERT INTO tab VALUES (1000, map('k', NULL));

SELECT count() FROM tab WHERE NOT startsWith(m['k'], 'foobar');
SELECT count() FROM tab WHERE NOT startsWith(m['k'], 'foobar') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE NOT m['k'] LIKE 'foobar%';
SELECT count() FROM tab WHERE NOT m['k'] LIKE 'foobar%' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE NOT endsWith(m['k'], 'value');
SELECT count() FROM tab WHERE NOT endsWith(m['k'], 'value') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE startsWith(m['k'], 'foobar');
SELECT count() FROM tab WHERE startsWith(m['k'], 'foobar') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
