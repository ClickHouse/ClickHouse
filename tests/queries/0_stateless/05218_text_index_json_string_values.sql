-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: direct read from the text index is not compatible with parallel replicas

-- Basic coverage for `tokenizer = jsonStringValues`: a text index on a bare JSON column that
-- stores String leaves as path-scoped tokens. Exact answers: typed String, typed Nullable(String),
-- and explicit `.:String`. Other haystacks stay Unknown.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    json JSON(status String, note Nullable(String)),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES
    (0, '{"status": "error", "note": "hello world", "msg": "ok"}'),
    (1, '{"status": "ok", "msg": "error"}'),
    (2, '{"status": "error", "score": 42, "msg": "timeout"}'),
    (3, '{"status": "ok", "tags": ["error", "fail"], "msg": "plain"}'),
    (4, '{"status": "ok", "note": "connection timeout", "score": "12"}');

SELECT '-- typed String: Exact';
SELECT 'idx', id FROM tab WHERE hasToken(json.status, 'error') ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.status, 'error') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- typed Nullable(String): Exact on a positive filter';
SELECT 'hello', id FROM tab WHERE hasToken(json.note, 'hello') ORDER BY id;
SELECT 'all', id FROM tab WHERE hasAllTokens(json.note, ['connection', 'timeout']) ORDER BY id;
SELECT 'any', id FROM tab WHERE hasAnyTokens(json.status, ['error', 'missing']) ORDER BY id;

SELECT '-- explicit .:String is path-scoped: error on msg, not on status or Array';
SELECT id FROM tab WHERE hasToken(json.msg.:`String`, 'error') ORDER BY id;

SELECT '-- numbers are not indexed; only a String leaf matches';
SELECT 'score 12', id FROM tab WHERE hasToken(json.score.:`String`, '12') ORDER BY id;
SELECT 'score 42', count() FROM tab WHERE hasToken(json.score.:`String`, '42');

SELECT '-- Array(String) is not recursed into';
SELECT count() FROM tab WHERE hasToken(json.tags.:`String`, 'error');

SELECT '-- AND of two Exact predicates';
SELECT id FROM tab WHERE hasToken(json.status, 'error') AND hasToken(json.msg.:`String`, 'ok') ORDER BY id;

SELECT '-- the index prunes granules';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(json.status, 'error'))
WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- Exact direct read replaces the predicate';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(json.status, 'error'))
WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- Exact direct read replaces positive Nullable and .:String';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(json.note, 'hello'))
WHERE explain LIKE '%__text_index_idx_hasToken%';
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(json.msg.:`String`, 'error'))
WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- positive count() is ReadFromTextIndexCount';
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(json.note, 'hello'))
WHERE explain LIKE '%ReadFromTextIndexCount%';
SELECT count() > 0
FROM (EXPLAIN SELECT count() FROM tab WHERE hasToken(json.msg.:`String`, 'error'))
WHERE explain LIKE '%ReadFromTextIndexCount%';

SELECT '-- Exact direct read also replaces under NOT';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE NOT hasToken(json.msg.:`String`, 'error'))
WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- IS FALSE over Nullable uses skip index (no direct read, matches master)';
SELECT 'idx', id FROM tab WHERE hasToken(json.note, 'hello') IS FALSE ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.note, 'hello') IS FALSE ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(json.note, 'hello') IS FALSE)
WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- NOT (IS FALSE) uses skip index (no direct read, matches master)';
SELECT 'idx', id FROM tab WHERE NOT (hasToken(json.note, 'hello') IS FALSE) ORDER BY id;
SELECT 'scan', id FROM tab WHERE NOT (hasToken(json.note, 'hello') IS FALSE) ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- equals 0 over Nullable uses skip index (no direct read, matches master)';
SELECT 'idx', id FROM tab WHERE hasToken(json.note, 'hello') = 0 ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.note, 'hello') = 0 ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(json.note, 'hello') = 0)
WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- notEquals 1 over Nullable uses skip index (no direct read, matches master)';
SELECT 'idx', id FROM tab WHERE hasToken(json.note, 'hello') != 1 ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.note, 'hello') != 1 ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE hasToken(json.note, 'hello') != 1)
WHERE explain LIKE '%__text_index_idx_hasToken%';

SELECT '-- bare Dynamic is Unknown: correct rows, but the index is not used';
SELECT 'idx', id FROM tab WHERE hasToken(json.msg, 'error') ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.msg, 'error') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasToken(json.msg, 'error'))
WHERE explain LIKE '%Name: idx%';
SELECT id FROM tab WHERE hasToken(json.msg, 'error') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT '-- NOT over Nullable uses Exact (NULL -> 0)';
SELECT 'idx', id FROM tab WHERE NOT hasToken(json.note, 'hello') ORDER BY id;
SELECT 'scan', id FROM tab WHERE NOT hasToken(json.note, 'hello') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE NOT hasToken(json.note, 'hello'))
WHERE explain LIKE '%Name: idx%';

SELECT '-- AND of typed String with NOT Nullable treats NULL as 0';
SELECT 'idx', id FROM tab WHERE hasToken(json.status, 'ok') AND NOT hasToken(json.note, 'hello') ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.status, 'ok') AND NOT hasToken(json.note, 'hello') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'idx', id FROM tab WHERE hasToken(json.status, 'ok') AND NOT hasToken(json.msg.:`String`, 'error') ORDER BY id;
SELECT 'scan', id FROM tab WHERE hasToken(json.status, 'ok') AND NOT hasToken(json.msg.:`String`, 'error') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- projected hasToken treats NULL as 0';
SELECT id, hasToken(json.note, 'hello') AS matched FROM tab WHERE matched OR id = 2 ORDER BY id;
SELECT id, hasToken(json.note, 'hello') AS matched FROM tab WHERE matched OR id = 2 ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '-- mixed parts: unmaterialized Nullable default throws (NULL -> UInt8 cast), matching master';
DROP TABLE IF EXISTS tab_partial;
CREATE TABLE tab_partial
(
    id UInt32,
    json JSON(note Nullable(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_partial VALUES
    (0, '{"note": "hello"}'),
    (1, '{"note": null}');

ALTER TABLE tab_partial ADD INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1;

INSERT INTO tab_partial VALUES
    (2, '{"note": "hello"}'),
    (3, '{"note": null}');

SELECT 'idx', id FROM tab_partial WHERE hasToken(json.note, 'hello') ORDER BY id; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT 'scan', id FROM tab_partial WHERE hasToken(json.note, 'hello') ORDER BY id SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM tab_partial WHERE hasToken(json.note, 'hello'))
WHERE explain LIKE '%__text_index_idx_hasToken%';

DROP TABLE tab_partial;

SELECT '-- nested JSON keeps the parent path prefix';
DROP TABLE IF EXISTS tab_nested;
CREATE TABLE tab_nested
(
    id UInt32,
    json JSON(a JSON(b String), extra JSON),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_nested VALUES
    (0, '{"a": {"b": "hit"}, "extra": {"c": "nested"}}'),
    (1, '{"a": {"b": "miss"}, "extra": {"c": "other"}}');

SELECT 'typed nested', id FROM tab_nested WHERE hasToken(json.a.b, 'hit') ORDER BY id;
SELECT 'typed nested scan', id FROM tab_nested WHERE hasToken(json.a.b, 'hit') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'nested dynamic', id FROM tab_nested WHERE hasToken(json.extra.c.:`String`, 'nested') ORDER BY id;
SELECT 'nested dynamic scan', id FROM tab_nested WHERE hasToken(json.extra.c.:`String`, 'nested') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'no relative-path false match', count() FROM tab_nested WHERE hasToken(json.b.:`String`, 'hit');

DROP TABLE tab_nested;

SELECT '-- Nullable(JSON) skips NULL rows and still indexes String leaves';
DROP TABLE IF EXISTS tab_null;
CREATE TABLE tab_null
(
    id UInt32,
    json Nullable(JSON(status String)),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_null VALUES
    (0, '{"status": "error"}'),
    (1, NULL),
    (2, '{"status": "ok"}');

SELECT 'idx', id FROM tab_null WHERE hasToken(json.status, 'error') ORDER BY id;
SELECT 'scan', id FROM tab_null WHERE hasToken(json.status, 'error') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'null row', id FROM tab_null WHERE json IS NULL ORDER BY id;

DROP TABLE tab_null;

SELECT '-- shared-data String paths: JSON(max_dynamic_paths = 0)';
DROP TABLE IF EXISTS tab_shared_paths;
CREATE TABLE tab_shared_paths
(
    id UInt32,
    json JSON(max_dynamic_paths = 0),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_shared_paths VALUES
    (0, '{"msg": "fromshared", "n": 42}'),
    (1, '{"msg": "other", "n": 1}');

SELECT 'idx', id FROM tab_shared_paths WHERE hasToken(json.msg.:`String`, 'fromshared') ORDER BY id;
SELECT 'scan', id FROM tab_shared_paths WHERE hasToken(json.msg.:`String`, 'fromshared') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'number not indexed', count() FROM tab_shared_paths WHERE hasToken(json.n.:`String`, '42');
SELECT 'no cross-path FP', count() FROM tab_shared_paths WHERE hasToken(json.n.:`String`, 'fromshared');

DROP TABLE tab_shared_paths;

SELECT '-- Dynamic shared variant: JSON(max_dynamic_types = 1) evicts the minority String';
DROP TABLE IF EXISTS tab_shared_variant;
CREATE TABLE tab_shared_variant
(
    id UInt32,
    json JSON(max_dynamic_types = 1),
    INDEX idx json TYPE text(tokenizer = 'jsonStringValues') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO tab_shared_variant VALUES
    (0, '{"msg": 1}'),
    (1, '{"msg": 2}'),
    (2, '{"msg": 3}'),
    (3, '{"msg": "evicted"}');

SELECT 'in shared variant', id, isDynamicElementInSharedData(json.msg) FROM tab_shared_variant ORDER BY id;
SELECT 'idx', id FROM tab_shared_variant WHERE hasToken(json.msg.:`String`, 'evicted') ORDER BY id;
SELECT 'scan', id FROM tab_shared_variant WHERE hasToken(json.msg.:`String`, 'evicted') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'number not indexed', count() FROM tab_shared_variant WHERE hasToken(json.msg.:`String`, '1');

DROP TABLE tab_shared_variant;

SELECT '-- rejected index definitions';
CREATE TABLE tab_bad (id UInt32, s String, INDEX idx s TYPE text(tokenizer = 'jsonStringValues')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, json JSON, INDEX idx JSONAllValues(json) TYPE text(tokenizer = 'jsonStringValues')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, json JSON, INDEX idx json TYPE text(tokenizer = 'jsonStringValues', preprocessor = lowerUTF8(json))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- the tokenizer does not tokenize strings';
SELECT tokens('a b', 'jsonStringValues'); -- { serverError NOT_IMPLEMENTED }
