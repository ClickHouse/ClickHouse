-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests `col IN (...)` on a text index with the `array` tokenizer: every value is one token, so the
-- union of the posting lists of the set elements answers the predicate (exact direct read).

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- Three granules: rows (1, 2), (3, 4) and (5, 6).
INSERT INTO tab VALUES (1, 'error'), (2, 'warn'), (3, 'error'), (4, 'info'), (5, 'debug'), (6, '');

SELECT '-- a list of values';
SELECT 'idx', id FROM tab WHERE s IN ('error', 'warn') ORDER BY id;
SELECT 'scan', id FROM tab WHERE s IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'count', count() FROM tab WHERE s IN ('error', 'warn');

SELECT '-- a single value behaves like equals';
SELECT 'idx', id FROM tab WHERE s IN ('info') ORDER BY id;
SELECT 'scan', id FROM tab WHERE s IN ('info') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- no value of the list matches';
SELECT 'idx', count() FROM tab WHERE s IN ('fatal', 'trace');
SELECT 'scan', count() FROM tab WHERE s IN ('fatal', 'trace') SETTINGS use_skip_indexes = 0;

SELECT '-- the index prunes granules';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE s IN ('debug')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- exact direct read: the predicate is replaced by a text index virtual column';
SELECT 'replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_in%';

SELECT '-- combined with AND, OR and NOT';
SELECT 'and idx', id FROM tab WHERE s IN ('error', 'warn') AND id > 1 ORDER BY id;
SELECT 'and scan', id FROM tab WHERE s IN ('error', 'warn') AND id > 1 ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'or idx', id FROM tab WHERE s IN ('debug') OR s IN ('info', 'warn') ORDER BY id;
SELECT 'or scan', id FROM tab WHERE s IN ('debug') OR s IN ('info', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not idx', id FROM tab WHERE s IN ('error', 'info') AND NOT (s IN ('error')) ORDER BY id;
SELECT 'not scan', id FROM tab WHERE s IN ('error', 'info') AND NOT (s IN ('error')) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not in idx', id FROM tab WHERE s NOT IN ('error', 'warn') ORDER BY id;
SELECT 'not in scan', id FROM tab WHERE s NOT IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- an OR chain of equals is folded into IN by the analyzer';
SELECT 'idx', id FROM tab WHERE s = 'error' OR s = 'warn' OR s = 'info' ORDER BY id;
SELECT 'scan', id FROM tab WHERE s = 'error' OR s = 'warn' OR s = 'info' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s = 'error' OR s = 'warn' OR s = 'info') WHERE explain LIKE '%__text_index_idx_in%';

SELECT '-- the empty string is not indexed, so the index is bypassed';
SELECT 'idx', id FROM tab WHERE s IN ('error', '') ORDER BY id;
SELECT 'scan', id FROM tab WHERE s IN ('error', '') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('error', '')) WHERE explain LIKE '%__text_index%';

SELECT '-- a tuple left-hand side is answered by a token union only as a hint';
SELECT 'idx', id FROM tab WHERE (s, id) IN (('error', 1), ('warn', 3)) ORDER BY id;
SELECT 'scan', id FROM tab WHERE (s, id) IN (('error', 1), ('warn', 3)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'hint', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE (s, id) IN (('error', 1), ('warn', 3))) WHERE explain LIKE '%__text_index_idx_in%';
SELECT 'original predicate', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE (s, id) IN (('error', 1), ('warn', 3))) WHERE explain LIKE '%FUNCTION in(%';

SELECT '-- a set from a subquery skips granules, but has no direct read';
SELECT 'idx', id FROM tab WHERE s IN (SELECT 'error') ORDER BY id;
SELECT 'scan', id FROM tab WHERE s IN (SELECT 'error') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'empty subquery', count() FROM tab WHERE s IN (SELECT 'error' WHERE 0);
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN (SELECT 'error')) WHERE explain LIKE '%__text_index%';

SELECT '-- transform_null_in = 1 produces nullIn, which is not read directly';
SELECT 'idx', id FROM tab WHERE s IN ('error', 'warn') ORDER BY id SETTINGS transform_null_in = 1;
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE s IN ('error', 'warn') SETTINGS transform_null_in = 1) WHERE explain LIKE '%__text_index%';

DROP TABLE tab;

SELECT '-- LowCardinality, Nullable and FixedString columns';

DROP TABLE IF EXISTS tab_types;

CREATE TABLE tab_types
(
    id UInt32,
    lc LowCardinality(String),
    n Nullable(String),
    f FixedString(5),
    INDEX idx_lc lc TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX idx_n n TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX idx_f f TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_types VALUES (1, 'error', 'error', 'error'), (2, 'warn', NULL, 'warn'), (3, 'info', 'info', 'info');

SELECT 'lc idx', id FROM tab_types WHERE lc IN ('error', 'warn') ORDER BY id;
SELECT 'lc scan', id FROM tab_types WHERE lc IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'lc replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_types WHERE lc IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_lc_in%';
SELECT 'n idx', id FROM tab_types WHERE n IN ('error', 'warn') ORDER BY id;
SELECT 'n scan', id FROM tab_types WHERE n IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'n replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_types WHERE n IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_n_in%';
SELECT 'not n idx', id FROM tab_types WHERE NOT (n IN ('error', 'warn')) ORDER BY id;
SELECT 'not n scan', id FROM tab_types WHERE NOT (n IN ('error', 'warn')) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'f idx', id FROM tab_types WHERE f IN ('error', 'warn') ORDER BY id;
SELECT 'f scan', id FROM tab_types WHERE f IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'f replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_types WHERE f IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_f_in%';

DROP TABLE tab_types;

SELECT '-- indexes that do not store the value as it is keep the predicate: direct read is a hint';

DROP TABLE IF EXISTS tab_other;

CREATE TABLE tab_other
(
    id UInt32,
    s String,
    m Map(String, String),
    INDEX idx_lower s TYPE text(tokenizer = 'array', preprocessor = lower(s)) GRANULARITY 1,
    INDEX idx_values mapValues(m) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_other VALUES (1, 'Error', {'level':'error'}), (2, 'error', {'other':'error'}), (3, 'warn', {'level':'warn'});

SELECT 'preprocessor idx', id FROM tab_other WHERE s IN ('error', 'warn') ORDER BY id;
SELECT 'preprocessor scan', id FROM tab_other WHERE s IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'preprocessor hint', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_other WHERE s IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_lower_in%';
SELECT 'preprocessor original predicate', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_other WHERE s IN ('error', 'warn')) WHERE explain LIKE '%FUNCTION in(%';
SELECT 'map values idx', id FROM tab_other WHERE m['level'] IN ('error', 'warn') ORDER BY id;
SELECT 'map values scan', id FROM tab_other WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'map values hint', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_other WHERE m['level'] IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_values_in%';
SELECT 'map values original predicate', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_other WHERE m['level'] IN ('error', 'warn')) WHERE explain LIKE '%FUNCTION in(%';

DROP TABLE tab_other;

SELECT '-- a part without a materialized index must not lose rows under exact direct read';

DROP TABLE IF EXISTS tab_partial;

CREATE TABLE tab_partial
(
    id UInt32,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab_partial;

-- This part predates the index, so the index is not materialized in it.
INSERT INTO tab_partial VALUES (1, 'error'), (2, 'warn');

ALTER TABLE tab_partial ADD INDEX idx s TYPE text(tokenizer = 'array') GRANULARITY 1;

-- These are written with the index in place.
INSERT INTO tab_partial VALUES (3, 'error'), (4, 'info');

SELECT 'some part has no materialized index', count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partial' AND active AND secondary_indices_marks_bytes = 0;
SELECT 'idx', id FROM tab_partial WHERE s IN ('error', 'warn') ORDER BY id;
SELECT 'scan', id FROM tab_partial WHERE s IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_partial WHERE s IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_in%';

DROP TABLE tab_partial;
