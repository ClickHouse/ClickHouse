-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- `arrayElement` on a `Map(K, LowCardinality(V))` returns `V`, while the subcolumn `m.key_<key>` is `LowCardinality(V)`.
-- Hence `optimize_functions_to_subcolumns` rewrites `m['key'] = 'value'` into `_CAST(m.key_<key>, 'V') = 'value'`,
-- and the text index has to look through the cast: for the `keyValuePairs` index on `m` and for an index on `mapValues(m)`.
-- The same applies to every conversion that cannot change a value or throw: adding or dropping `LowCardinality`,
-- adding `Nullable` (also as `toNullable`, which `join_use_nulls` emits in pushed-down filters), at any depth of `Array`.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_kv;

CREATE TABLE tab_kv
(
    id UInt32,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- One part with two granules: rows (1, 2) and rows (3, 4).
INSERT INTO tab_kv VALUES (1, {'level':'error','service':'api'}), (2, {'level':'warn','service':'api'}), (3, {'level':'error','service':'web'}), (4, {});

SELECT '-- the analyzer wraps the subcolumn into a cast to String';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT id FROM tab_kv WHERE m['level'] = 'warn') WHERE explain LIKE '%function_name: _CAST%';

SELECT '-- keyValuePairs: the index prunes granules through the cast';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_kv WHERE m['level'] = 'warn') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- keyValuePairs: exact direct read replaces the predicate';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_kv WHERE m['level'] = 'error') WHERE explain LIKE '%__text_index_idx_equals%';

SELECT '-- keyValuePairs: results match the scan';
SELECT 'idx', id FROM tab_kv WHERE m['level'] = 'error' ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_kv WHERE m['level'] = 'error' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'idx', id FROM tab_kv WHERE 'web' = m['service'] ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_kv WHERE 'web' = m['service'] ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'idx', count() FROM tab_kv WHERE m['level'] = 'api' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'idx', count() FROM tab_kv WHERE m['nope'] = 'error' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_kv;

DROP TABLE IF EXISTS tab_values;

CREATE TABLE tab_values
(
    id UInt32,
    m Map(String, LowCardinality(String)),
    INDEX idx mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_values VALUES (1, {'level':'error','msg':'disk is full'}), (2, {'level':'warn','msg':'disk is slow'}), (3, {'level':'error','msg':'network is down'}), (4, {});

SELECT '-- mapValues: the index prunes granules through the cast';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_values WHERE m['level'] = 'warn') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_values WHERE hasToken(m['msg'], 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- mapValues: results match the scan';
SELECT 'idx', id FROM tab_values WHERE m['level'] = 'error' ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_values WHERE m['level'] = 'error' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'idx', id FROM tab_values WHERE hasToken(m['msg'], 'disk') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_values WHERE hasToken(m['msg'], 'disk') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'idx', id FROM tab_values WHERE m['msg'] LIKE '%down%' ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_values WHERE m['msg'] LIKE '%down%' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT '-- mapValues is only a hint: a value that belongs to another key does not match';
SELECT 'idx', count() FROM tab_values WHERE m['level'] = 'disk' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_values;

-- A `mapValues` index on `LowCardinality(Nullable(String))` values: the analyzer casts to `Nullable(String)`.
DROP TABLE IF EXISTS tab_values_n;

CREATE TABLE tab_values_n
(
    id UInt32,
    m Map(String, LowCardinality(Nullable(String))),
    INDEX idx mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_values_n VALUES (1, {'level':'error'}), (2, {'level':'error'}), (3, {'level':'warn'}), (4, {'level':NULL});

SELECT '-- mapValues on LowCardinality(Nullable) values: the cast to Nullable(String) is looked through';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_values_n WHERE m['level'] = 'warn') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'idx', id FROM tab_values_n WHERE m['level'] = 'warn' ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_values_n WHERE m['level'] = 'warn' ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_values_n;

-- Plain columns with explicit conversions.
DROP TABLE IF EXISTS tab_s;

CREATE TABLE tab_s
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_s VALUES (1, 'disk is full'), (2, 'disk is slow'), (3, 'network is down'), (4, '');

SELECT '-- String column: conversions that only add Nullable or LowCardinality are looked through';
SELECT 'CAST Nullable', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_s WHERE hasToken(CAST(s, 'Nullable(String)'), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'CAST LowCardinality', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_s WHERE hasToken(CAST(s, 'LowCardinality(String)'), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'toNullable', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_s WHERE hasToken(toNullable(s), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'toLowCardinality', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_s WHERE hasToken(toLowCardinality(s), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'nested', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_s WHERE hasToken(toNullable(toLowCardinality(s)), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'idx', id FROM tab_s WHERE hasToken(toNullable(s), 'disk') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_s WHERE hasToken(toNullable(s), 'disk') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not idx', id FROM tab_s WHERE NOT hasToken(toNullable(s), 'disk') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'not scan', id FROM tab_s WHERE NOT hasToken(toNullable(s), 'disk') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'in idx', id FROM tab_s WHERE toNullable(s) IN ('disk is full', 'network is down') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'in scan', id FROM tab_s WHERE toNullable(s) IN ('disk is full', 'network is down') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- join_use_nulls pushes the filter down to the outer side as toNullable(column)';
DROP TABLE IF EXISTS tab_ids;
CREATE TABLE tab_ids (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_ids SELECT number FROM numbers(1, 4);
-- `query_plan_convert_outer_join_to_inner_join` is pinned because the filter is pushed below the join only
-- when the LEFT JOIN becomes an INNER one; otherwise it stays above the join and no index is consulted.
SELECT count() FROM (EXPLAIN indexes = 1 SELECT l.id FROM tab_ids AS l LEFT JOIN tab_s AS r ON l.id = r.id WHERE hasToken(r.s, 'network') SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1) WHERE explain LIKE '%Name: idx%';
SELECT l.id FROM tab_ids AS l LEFT JOIN tab_s AS r ON l.id = r.id WHERE hasToken(r.s, 'network') ORDER BY l.id SETTINGS join_use_nulls = 1;
DROP TABLE tab_ids;

DROP TABLE tab_s;

DROP TABLE IF EXISTS tab_ns;

CREATE TABLE tab_ns
(
    id UInt32,
    s Nullable(String),
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_ns VALUES (1, 'disk is full'), (2, 'disk is slow'), (3, 'network is down'), (4, NULL);

SELECT '-- Nullable column: adding LowCardinality is looked through';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_ns WHERE hasToken(CAST(s, 'LowCardinality(Nullable(String))'), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'idx', id FROM tab_ns WHERE hasToken(CAST(s, 'LowCardinality(Nullable(String))'), 'disk') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_ns WHERE hasToken(CAST(s, 'LowCardinality(Nullable(String))'), 'disk') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- Nullable column: dropping Nullable is not looked through, the cast still throws on the NULL row';
SELECT count() FROM (EXPLAIN indexes = 1 SELECT id FROM tab_ns WHERE hasToken(CAST(s, 'String'), 'network')) WHERE explain LIKE '%Name: idx%';
SELECT id FROM tab_ns WHERE hasToken(CAST(s, 'String'), 'network'); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

DROP TABLE tab_ns;

DROP TABLE IF EXISTS tab_arr;

CREATE TABLE tab_arr
(
    id UInt32,
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_arr VALUES (1, ['disk', 'full']), (2, ['disk', 'slow']), (3, ['network', 'down']), (4, []);

SELECT '-- Array column: a cast that only wraps the elements is looked through';
SELECT 'Array(Nullable)', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_arr WHERE has(CAST(arr, 'Array(Nullable(String))'), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'Array(LowCardinality)', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_arr WHERE has(CAST(arr, 'Array(LowCardinality(String))'), 'network')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT 'idx', id FROM tab_arr WHERE has(CAST(arr, 'Array(Nullable(String))'), 'disk') ORDER BY id SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scan', id FROM tab_arr WHERE has(CAST(arr, 'Array(Nullable(String))'), 'disk') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_arr;
