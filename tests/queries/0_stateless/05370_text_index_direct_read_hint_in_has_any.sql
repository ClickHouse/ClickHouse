-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests direct read from the text index as a hint for `IN` and `hasAny` on an index with a tokenizer that
-- splits strings. Every element is a search of its own: its tokens are necessary, but not sufficient for a
-- match. The searches are read into separate virtual columns, OR-ed, and the original predicate is kept.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1.0;
SET query_plan_text_index_direct_read_max_branches = 16;
SET transform_null_in = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s String,
    arr Array(String),
    INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_arr arr TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

-- 'connection reset' rows also hold the words of 'reset connection' and 'connection', so the tokens of an
-- element select a superset of the rows that are equal to it.
INSERT INTO tab SELECT
    number,
    multiIf(number % 40 = 0, 'connection reset', number % 40 = 1, 'reset connection', number % 40 = 2, 'connection', number % 40 = 3, 'disk full', concat('message ', toString(number))),
    multiIf(number % 40 = 0, ['connection reset', 'other'], number % 40 = 1, ['reset', 'connection'], number % 40 = 3, ['disk full'], [concat('value ', toString(number))])
FROM numbers(4000);

SELECT '-- IN: results are the same with and without the index';

SELECT 'in',
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full')),
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full') SETTINGS query_plan_direct_read_from_text_index = 0);

SELECT 'in, one element',
    (SELECT count() FROM tab WHERE s IN ('connection reset') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN ('connection reset')),
    (SELECT count() FROM tab WHERE s IN ('connection reset') SETTINGS query_plan_direct_read_from_text_index = 0);

SELECT 'tuple in',
    (SELECT count() FROM tab WHERE (s, id % 2) IN (('connection reset', 0), ('disk full', 1)) SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE (s, id % 2) IN (('connection reset', 0), ('disk full', 1)));

SELECT 'not in',
    (SELECT count() FROM tab WHERE s NOT IN ('connection reset', 'disk full') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s NOT IN ('connection reset', 'disk full'));

SELECT 'not (in)',
    (SELECT count() FROM tab WHERE NOT (s IN ('connection reset', 'disk full')) SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE NOT (s IN ('connection reset', 'disk full')));

SELECT 'in, or',
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full') OR id = 5 SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full') OR id = 5);

SELECT '-- IN: one virtual column per element, OR-ed and AND-ed with the original predicate';

SELECT 'virtual columns', count() FROM
(
    SELECT DISTINCT arrayJoin(extractAll(explain, '__text_index_idx_s_in_[0-9a-f]{32}')) AS column
    FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', 'disk full'))
);
SELECT 'or', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', 'disk full')) WHERE explain LIKE '%FUNCTION or(__text_index_idx_s_in_%';
SELECT 'original predicate', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', 'disk full')) WHERE explain LIKE '%FUNCTION in(%';
SELECT 'tuple in', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE (s, id % 2) IN (('connection reset', 0), ('disk full', 1))) WHERE explain LIKE '%__text_index_idx_s_in_%';
SELECT 'not in', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s NOT IN ('connection reset', 'disk full')) WHERE explain LIKE '%__text_index%';

SELECT '-- IN: no direct read';

SELECT 'subquery', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN (SELECT 'connection reset')) WHERE explain LIKE '%__text_index%';
SELECT 'subquery results',
    (SELECT count() FROM tab WHERE s IN (SELECT 'connection reset') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN (SELECT 'connection reset'));
SELECT 'empty element', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', '')) WHERE explain LIKE '%__text_index%';
SELECT 'element without tokens', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', '...')) WHERE explain LIKE '%__text_index%';
SELECT 'element without tokens results',
    (SELECT count() FROM tab WHERE s IN ('connection reset', '...') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN ('connection reset', '...'));
SELECT 'above the limit', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', 'disk full', 'connection') SETTINGS query_plan_text_index_direct_read_max_branches = 2) WHERE explain LIKE '%__text_index%';
SELECT 'above the limit results',
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full', 'connection') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full', 'connection') SETTINGS query_plan_text_index_direct_read_max_branches = 2);
SELECT 'add_hint = 0', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('connection reset', 'disk full') SETTINGS query_plan_text_index_add_hint = 0) WHERE explain LIKE '%__text_index%';

SELECT '-- hasAny';

SELECT 'hasAny',
    (SELECT count() FROM tab WHERE hasAny(arr, ['connection reset', 'disk full']) SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE hasAny(arr, ['connection reset', 'disk full'])),
    (SELECT count() FROM tab WHERE hasAny(arr, ['connection reset', 'disk full']) SETTINGS query_plan_direct_read_from_text_index = 0);
SELECT 'not hasAny',
    (SELECT count() FROM tab WHERE NOT hasAny(arr, ['connection reset', 'disk full']) SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE NOT hasAny(arr, ['connection reset', 'disk full']));
SELECT 'virtual columns', count() FROM
(
    SELECT DISTINCT arrayJoin(extractAll(explain, '__text_index_idx_arr_hasAny_[0-9a-f]{32}')) AS column
    FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasAny(arr, ['connection reset', 'disk full']))
);
SELECT 'original predicate', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasAny(arr, ['connection reset', 'disk full'])) WHERE explain LIKE '%FUNCTION hasAny(%';

SELECT '-- the hint is used';

SELECT 'dread_in', count() FROM tab WHERE s IN ('connection reset', 'disk full');
SELECT 'dread_hasAny', count() FROM tab WHERE hasAny(arr, ['connection reset', 'disk full']);

SYSTEM FLUSH LOGS query_log;

SELECT
    extract(query, 'dread_[A-Za-z0-9]+') AS q,
    sum(ProfileEvents['TextIndexUseHint']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE 'SELECT ''dread_%'
GROUP BY q
ORDER BY q;

SELECT '-- make_distributed_plan';

SELECT 'in',
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE s IN ('connection reset', 'disk full')
        SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
            distributed_plan_default_reader_bucket_count = 3, distributed_plan_default_shuffle_join_bucket_count = 3,
            distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0);
SELECT 'hasAny',
    (SELECT count() FROM tab WHERE hasAny(arr, ['connection reset', 'disk full']) SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab WHERE hasAny(arr, ['connection reset', 'disk full'])
        SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
            distributed_plan_default_reader_bucket_count = 3, distributed_plan_default_shuffle_join_bucket_count = 3,
            distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0);

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

INSERT INTO tab_partial SELECT number, if(number % 10 = 0, 'connection reset', 'reset connection') FROM numbers(100);
ALTER TABLE tab_partial ADD INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1;
INSERT INTO tab_partial SELECT number + 100, if(number % 10 = 0, 'connection reset', 'disk full') FROM numbers(100);

SELECT 'results',
    (SELECT count() FROM tab_partial WHERE s IN ('connection reset', 'disk full') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM tab_partial WHERE s IN ('connection reset', 'disk full'));
SELECT 'replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_partial WHERE s IN ('connection reset', 'disk full')) WHERE explain LIKE '%__text_index%';

DROP TABLE tab_partial;
