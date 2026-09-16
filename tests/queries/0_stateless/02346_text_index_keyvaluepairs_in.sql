-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests `m['key'] IN (...)` on a text index with the `keyValuePairs` tokenizer: every value of the set
-- becomes one (key, value) token and the union of their posting lists is the result, so the predicate
-- is answered by the index alone.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- Three granules: rows (1, 2), (3, 4) and (5, 6). Row 6 has no pairs at all.
INSERT INTO tab VALUES (1, {'level':'error','service':'api'}), (2, {'level':'warn','service':'api'}), (3, {'level':'error','service':'web'}), (4, {'level':'info','service':'web'}), (5, {'level':'debug','service':'api'}), (6, {});

SELECT '-- a list of values';
SELECT 'idx', id FROM tab WHERE m['level'] IN ('error', 'warn') ORDER BY id;
SELECT 'scan', id FROM tab WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- a single value behaves like equals';
SELECT 'idx', id FROM tab WHERE m['level'] IN ('info') ORDER BY id;
SELECT 'scan', id FROM tab WHERE m['level'] IN ('info') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- no value of the list matches';
SELECT 'idx', count() FROM tab WHERE m['level'] IN ('fatal', 'trace');
SELECT 'scan', count() FROM tab WHERE m['level'] IN ('fatal', 'trace') SETTINGS use_skip_indexes = 0;

SELECT '-- the values belong to another key: no cross-pair match';
SELECT 'idx', count() FROM tab WHERE m['level'] IN ('api', 'web');
SELECT 'scan', count() FROM tab WHERE m['level'] IN ('api', 'web') SETTINGS use_skip_indexes = 0;

SELECT '-- a key that does not exist';
SELECT 'idx', count() FROM tab WHERE m['nope'] IN ('error', 'warn');
SELECT 'scan', count() FROM tab WHERE m['nope'] IN ('error', 'warn') SETTINGS use_skip_indexes = 0;

SELECT '-- the index prunes granules';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN ('debug')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- exact direct read: the predicate is replaced by a text index virtual column';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] IN ('error', 'warn')) WHERE explain LIKE '%__text_index_idx_in%';

SELECT '-- both accessor forms work: arrayElement and the m.key_<key> subcolumn';
SELECT 'subcolumns=0', id FROM tab WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'subcolumns=1', id FROM tab WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'subcolumns=0', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] IN ('error', 'warn') SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%__text_index_idx_in%';
SELECT 'subcolumns=1', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] IN ('error', 'warn') SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain LIKE '%__text_index_idx_in%';

SELECT '-- combined with AND, OR and NOT';
SELECT 'and idx', id FROM tab WHERE m['level'] IN ('error', 'warn') AND m['service'] = 'web' ORDER BY id;
SELECT 'and scan', id FROM tab WHERE m['level'] IN ('error', 'warn') AND m['service'] = 'web' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'or idx', id FROM tab WHERE m['level'] IN ('debug') OR m['service'] IN ('web') ORDER BY id;
SELECT 'or scan', id FROM tab WHERE m['level'] IN ('debug') OR m['service'] IN ('web') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not idx', id FROM tab WHERE NOT (m['level'] IN ('error', 'warn')) ORDER BY id;
SELECT 'not scan', id FROM tab WHERE NOT (m['level'] IN ('error', 'warn')) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not in idx', id FROM tab WHERE m['level'] NOT IN ('error', 'warn') ORDER BY id;
SELECT 'not in scan', id FROM tab WHERE m['level'] NOT IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- the empty string is also the value of a missing key, so the index is bypassed';
SELECT 'idx', id FROM tab WHERE m['level'] IN ('error', '') ORDER BY id;
SELECT 'scan', id FROM tab WHERE m['level'] IN ('error', '') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] IN ('error', '')) WHERE explain LIKE '%__text_index%';

SELECT '-- a FixedString element keeps its padding, which IN compares as part of the value';
SELECT 'idx', count() FROM tab WHERE m['level'] IN (toFixedString('error', 8));
SELECT 'scan', count() FROM tab WHERE m['level'] IN (toFixedString('error', 8)) SETTINGS use_skip_indexes = 0;

SELECT '-- a tuple left-hand side is not answered by a token union';
SELECT 'idx', id FROM tab WHERE (m['level'], id) IN (('error', 1), ('warn', 3)) ORDER BY id;
SELECT 'scan', id FROM tab WHERE (m['level'], id) IN (('error', 1), ('warn', 3)) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE (m['level'], id) IN (('error', 1), ('warn', 3))) WHERE explain LIKE '%__text_index%';

SELECT '-- a set from a subquery skips granules, but has no direct read';
SELECT 'idx', id FROM tab WHERE m['level'] IN (SELECT 'error') ORDER BY id;
SELECT 'scan', id FROM tab WHERE m['level'] IN (SELECT 'error') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'no subquery index', id FROM tab WHERE m['level'] IN (SELECT 'error') ORDER BY id SETTINGS use_index_for_in_with_subqueries = 0;
SELECT 'empty subquery', count() FROM tab WHERE m['level'] IN (SELECT 'error' WHERE 0);
-- The set has no AST representation, so `convertNodeToAST` returns nothing and direct read is skipped
-- altogether. Were it replaced by a virtual column, a part whose index is not materialized would have
-- no default expression to fall back on. The `tab_partial` case below covers that part.
SELECT 'skip index used', count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN (SELECT 'error')) WHERE explain LIKE '%Name: idx%';
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] IN (SELECT 'error')) WHERE explain LIKE '%__text_index%';

SELECT '-- a set from an ENGINE = Set table can change under the query, so the index must not answer it';
DROP TABLE IF EXISTS live_set;
CREATE TABLE live_set (v String) ENGINE = Set;
INSERT INTO live_set VALUES ('error'), ('warn');
SELECT 'idx', id FROM tab WHERE m['level'] IN live_set ORDER BY id;
SELECT 'scan', id FROM tab WHERE m['level'] IN live_set ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'no skip index', count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] IN live_set) WHERE explain LIKE '%Name: idx%';
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] IN live_set) WHERE explain LIKE '%__text_index%';
DROP TABLE live_set;

SELECT '-- an OR chain of equals is folded into IN by the analyzer';
SELECT 'idx', id FROM tab WHERE m['level'] = 'error' OR m['level'] = 'warn' OR m['level'] = 'info' ORDER BY id;
SELECT 'scan', id FROM tab WHERE m['level'] = 'error' OR m['level'] = 'warn' OR m['level'] = 'info' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'folded', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] = 'error' OR m['level'] = 'warn' OR m['level'] = 'info') WHERE explain LIKE '%__text_index_idx_in%';

SELECT '-- values with arbitrary bytes';
SELECT 'idx', count() FROM tab WHERE m['level'] IN ('error\0', 'error');
SELECT 'scan', count() FROM tab WHERE m['level'] IN ('error\0', 'error') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- duplicate keys: m[key] is the value of the first occurrence';

DROP TABLE IF EXISTS tab_dup;

CREATE TABLE tab_dup
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_dup VALUES (1, map('k', 'first', 'k', 'second')), (2, map('k', 'second')), (3, map('k', 'third'));

SELECT 'idx', id FROM tab_dup WHERE m['k'] IN ('first', 'third') ORDER BY id;
SELECT 'scan', id FROM tab_dup WHERE m['k'] IN ('first', 'third') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'idx', id FROM tab_dup WHERE m['k'] IN ('second') ORDER BY id;
SELECT 'scan', id FROM tab_dup WHERE m['k'] IN ('second') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_dup;

SELECT '-- LowCardinality keys and values';

DROP TABLE IF EXISTS tab_lc;

CREATE TABLE tab_lc
(
    id UInt32,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_lc VALUES (1, {'level':'error'}), (2, {'level':'warn'}), (3, {'level':'info'});

SELECT 'idx', id FROM tab_lc WHERE m['level'] IN ('error', 'info') ORDER BY id;
SELECT 'scan', id FROM tab_lc WHERE m['level'] IN ('error', 'info') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_lc;

SELECT '-- a part without a materialized index must not lose rows under exact direct read';

DROP TABLE IF EXISTS tab_partial;

CREATE TABLE tab_partial
(
    id UInt32,
    m Map(String, String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab_partial;

-- This part predates the index, so the index is not materialized in it.
INSERT INTO tab_partial VALUES (1, {'level':'error'}), (2, {'level':'warn'});

ALTER TABLE tab_partial ADD INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1;

-- These are written with the index in place.
INSERT INTO tab_partial VALUES (3, {'level':'error'}), (4, {'level':'info'});

SELECT 'some part has no materialized index', count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partial' AND active AND secondary_indices_marks_bytes = 0;
SELECT 'subcolumns=0', id FROM tab_partial WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'subcolumns=1', id FROM tab_partial WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'scan', id FROM tab_partial WHERE m['level'] IN ('error', 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;

-- A subquery set has no direct read, so the part without a materialized index is read as usual. This
-- is the combination an accidental exact rewrite would break, because such a set has no AST to fall
-- back on.
SELECT 'subquery', id FROM tab_partial WHERE m['level'] IN (SELECT 'error' UNION ALL SELECT 'warn') ORDER BY id;
SELECT 'subquery scan', id FROM tab_partial WHERE m['level'] IN (SELECT 'error' UNION ALL SELECT 'warn') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_partial;
