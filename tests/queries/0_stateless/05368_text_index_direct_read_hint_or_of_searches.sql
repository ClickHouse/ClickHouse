-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests direct read from the text index as a hint for the OR-of-searches functions `match`,
-- `multiSearchAny`, `multiSearchAnyUTF8` and `multiMatchAny`. Each search is read into its own virtual
-- column, the columns are OR-ed, and the original predicate is AND-ed on top.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1.0; -- always keep the hint, so that `TextIndexUseHint` is deterministic
SET query_plan_text_index_direct_read_max_branches = 16;

DROP TABLE IF EXISTS tab;

-- ngrams(3): the n-grams of a needle are required tokens, so a substring or a regexp search can use the index.
CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- 'foobar' and 'helloworld' are in disjoint rows, so an OR of the two needles returns both groups
-- (160 + 160 = 320). An AND of the searches would return no rows.
INSERT INTO tab SELECT
    number,
    multiIf(number % 50 = 0, 'foobar', number % 50 = 1, 'helloworld', concat('filler', toString(number)))
FROM numbers(8000);

SELECT '-- results are the same with and without direct read';

SELECT 'multiSearchAny',
    (SELECT count() FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld']) SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld']));

SELECT 'multiSearchAnyUTF8',
    (SELECT count() FROM tab WHERE multiSearchAnyUTF8(s, ['foobar', 'helloworld']) SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE multiSearchAnyUTF8(s, ['foobar', 'helloworld']));

SELECT 'match',
    (SELECT count() FROM tab WHERE match(s, 'foobar|helloworld') SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE match(s, 'foobar|helloworld'));

SELECT 'multiMatchAny',
    (SELECT count() FROM tab WHERE multiMatchAny(s, ['foobar', 'helloworld']) SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE multiMatchAny(s, ['foobar', 'helloworld']));

SELECT 'NOT match',
    (SELECT count() FROM tab WHERE NOT match(s, 'foobar|helloworld') SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE NOT match(s, 'foobar|helloworld'));

SELECT '-- one virtual column per search, OR-ed and AND-ed with the original predicate';

SELECT 'virtual columns', count() FROM
(
    SELECT DISTINCT arrayJoin(extractAll(explain, '__text_index_idx_match_[0-9a-f]{32}')) AS column
    FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE match(s, 'foobar|helloworld'))
);
SELECT 'or', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE match(s, 'foobar|helloworld')) WHERE explain LIKE '%FUNCTION or(__text_index_idx_match_%';
SELECT 'original predicate', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE match(s, 'foobar|helloworld')) WHERE explain LIKE '%FUNCTION match(%';

SELECT '-- the hint is used for each function';

SELECT 'dread_multiSearchAny',     count() FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld']);
SELECT 'dread_multiSearchAnyUTF8', count() FROM tab WHERE multiSearchAnyUTF8(s, ['foobar', 'helloworld']);
SELECT 'dread_match',              count() FROM tab WHERE match(s, 'foobar|helloworld');
SELECT 'dread_multiMatchAny',      count() FROM tab WHERE multiMatchAny(s, ['foobar', 'helloworld']);

SYSTEM FLUSH LOGS query_log;

SELECT
    extract(query, 'dread_[A-Za-z0-9]+') AS q,
    sum(ProfileEvents['TextIndexUseHint']) > 0,
    sum(ProfileEvents['TextIndexDiscardHint'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE 'SELECT ''dread_%'
GROUP BY q
ORDER BY q;

SELECT '-- a search without tokens matches every row, so there is no direct read';

SELECT 'match with .*',
    (SELECT count() FROM tab WHERE match(s, 'foobar|.*') SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE match(s, 'foobar|.*'));
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE match(s, 'foobar|.*')) WHERE explain LIKE '%__text_index%';

SELECT 'needle shorter than an n-gram',
    (SELECT count() FROM tab WHERE multiSearchAny(s, ['foobar', 'he']) SETTINGS query_plan_direct_read_from_text_index = 0),
    (SELECT count() FROM tab WHERE multiSearchAny(s, ['foobar', 'he']));
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE multiSearchAny(s, ['foobar', 'he'])) WHERE explain LIKE '%__text_index%';

SELECT '-- query_plan_text_index_direct_read_max_branches';

SELECT '3 needles, limit 3', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld', 'filler1']) SETTINGS query_plan_text_index_direct_read_max_branches = 3) WHERE explain LIKE '%__text_index%';
SELECT '3 needles, limit 2', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld', 'filler1']) SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_direct_read_max_branches = 2) WHERE explain LIKE '%__text_index%';
SELECT '3 needles, no limit', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld', 'filler1']) SETTINGS query_plan_text_index_direct_read_max_branches = 0) WHERE explain LIKE '%__text_index%';
SELECT '17 needles, default limit', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE multiSearchAny(s, arrayMap(x -> concat('filler', toString(x)), range(17)))) WHERE explain LIKE '%__text_index%';
SELECT '17 needles, results',
    (SELECT count() FROM tab WHERE multiSearchAny(s, arrayMap(x -> concat('filler', toString(x + 100)), range(17))) SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE multiSearchAny(s, arrayMap(x -> concat('filler', toString(x + 100)), range(17))));
SELECT 'above the limit, granules are skipped', count() > 0 FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE multiSearchAny(s, ['foobar', 'helloworld', 'filler1']) SETTINGS query_plan_text_index_direct_read_max_branches = 2) WHERE explain LIKE '%Name: idx%';

SELECT '-- without hints there is no direct read';

SELECT 'add_hint = 0', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE match(s, 'foobar|helloworld') SETTINGS query_plan_text_index_add_hint = 0) WHERE explain LIKE '%__text_index%';

DROP TABLE tab;

SELECT '-- a part without a materialized index';

DROP TABLE IF EXISTS tab_partial;

CREATE TABLE tab_partial
(
    id UInt32,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

SYSTEM STOP MERGES tab_partial;

INSERT INTO tab_partial SELECT number, if(number % 10 = 0, 'foobar', 'helloworld') FROM numbers(100);
ALTER TABLE tab_partial ADD INDEX idx(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1;
INSERT INTO tab_partial SELECT number + 100, if(number % 10 = 0, 'foobar', 'other') FROM numbers(100);

SELECT 'results',
    (SELECT count() FROM tab_partial WHERE match(s, 'foobar|helloworld') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab_partial WHERE match(s, 'foobar|helloworld'));
SELECT 'replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_partial WHERE match(s, 'foobar|helloworld')) WHERE explain LIKE '%__text_index%';

DROP TABLE tab_partial;
